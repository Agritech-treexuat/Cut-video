import boto3
import datetime
from pymongo import MongoClient
import threading
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_REGION = os.getenv("WASABI_REGION")
WASABI_ENDPOINT_URL = os.getenv("WASABI_ENDPOINT_URL")
WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")

# # Khai báo thông tin kết nối MongoDB
MONGO_CONNECTION_STRING = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"

# Hàm để lấy 3h gần nhất từ MongoDB
def get_recent_3_hours_from_mongodb():
    # # Kết nối đến MongoDB
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    # # Lấy thời gian hiện tại
    # current_time = datetime.datetime.now()

    # # Tính thời gian trước 3 giờ
    # three_hours_ago = current_time - datetime.timedelta(hours=3)

    # # Lọc các bản ghi trong khoảng thời gian từ three_hours_ago đến current_time
    # query = {"start": {"$gte": three_hours_ago}, "end": {"$lte": current_time}}
    # result = collection.find(query)

    # # Format kết quả thành list của các object với các trường cameraId và time
    # recent_3_hours = []
    # for doc in result:
    #     recent_3_hours.append({"cameraId": doc["cameraId"], "times": [{"start": doc["start"], "end": doc["end"]}]})

    # sample of recent 3 hours
    recent_3_hours = [ 
        {
            "cameraId": "camera-1",
            "times": [
                {
                    "start": datetime.datetime(2021, 8, 25, 0, 0, 0),
                    "end": datetime.datetime(2021, 8, 25, 0, 5, 0)
                },
                {
                    "start": datetime.datetime(2021, 8, 25, 0, 5, 0),
                    "end": datetime.datetime(2021, 8, 25, 0, 10, 0)
                }
            ]
        }
    ]

    return recent_3_hours

# Hàm thực hiện cắt và tải video
def process_video(camera_id, times):
    # Khởi tạo client của AWS S3
    s3 = boto3.client('s3',
                      endpoint_url=WASABI_ENDPOINT_URL,
                      aws_access_key_id=WASABI_ACCESS_KEY,
                      aws_secret_access_key=WASABI_SECRET_KEY,
                      region_name=WASABI_REGION)

    # Tạo đường dẫn tới thư mục của camera trên Wasabi
    camera_folder = f"{WASABI_BUCKET_NAME}/{camera_id}"

    # Tạo đường dẫn tới thư mục 24h trên Wasabi
    twenty_four_hours_folder = f"{WASABI_BUCKET_NAME}/24h/{camera_id}"

    # Lấy danh sách video từ thư mục 24h
    objects = s3.list_objects_v2(Bucket=WASABI_BUCKET_NAME, Prefix=f"24h/{camera_id}")

    if 'Contents' in objects:
        for obj in objects['Contents']:
            # Lấy tên video
            video_name = obj['Key']
            print("video name: ", video_name)
            if video_name.endswith("/"):
                continue

            # get video original name form video_name
            video_original_name = video_name.split('/')[-1]

            # check if exist folder tmp/video_name
            if not os.path.exists('./tmp'):
                os.makedirs('./tmp')
            if not os.path.exists(f'./tmp/{camera_id}'):
                os.makedirs(f'./tmp/{camera_id}')
            
            
            # download video use boto3
            s3.download_file(WASABI_BUCKET_NAME, video_name, f"./tmp/{camera_id}/{video_original_name}")

            # Cắt video và tải lên Wasabi
            for time in times:
                start_time = time["start"]
                end_time = time["end"]
                start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
                end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
                
                start_time_str = '00:00:00'
                end_time_str = '00:00:05'
                # Tạo tên video mới
                new_video_name = f"{start_time_str}_{end_time_str}_{video_original_name}"
                new_video_path = f"./tmp/{new_video_name}"
                # Cắt video
                command = f"ffmpeg -i ./tmp/{camera_id}/{video_original_name} -ss {start_time_str} -to {end_time_str} -c copy {new_video_path}"
                os.system(command)
                # Tải video lên Wasabi
                s3.upload_file(f"{new_video_path}", WASABI_BUCKET_NAME, f"video/{camera_id}/{new_video_name}")

                # Xóa video cắt
                os.remove(f"{new_video_path}")
            
            # Xóa video gốc
            os.remove(f"./tmp/{camera_id}/{video_original_name}")
            # delete video from wasabi
            s3.delete_object(Bucket=WASABI_BUCKET_NAME, Key=video_name)

# Hàm chính để xử lý
def main():
    # Lấy 3h gần nhất từ MongoDB
    recent_3_hours = get_recent_3_hours_from_mongodb()

    # Duyệt qua từng object trong list recent_3_hours
    for obj in recent_3_hours:
        camera_id = obj["cameraId"]
        times = obj["times"]
        # Tạo và chạy thread để xử lý video của camera tương ứng
        thread = threading.Thread(target=process_video, args=(camera_id, times))
        thread.start()

if __name__ == "__main__":
    main()
