import boto3
import os
import datetime
import subprocess

# Khởi tạo client của AWS S3
s3 = boto3.client('s3',
                  endpoint_url='https://s3.ap-southeast-1.wasabisys.com',
                  aws_access_key_id='T07LPLGBGBX80SLVA1X4',
                  aws_secret_access_key='1Wasuk6Pbz9i8T3YjU4eQvysBBMdpU0U68MYFpmr',
                  region_name='ap-southeast-1')


# Lấy danh sách tệp video trong 24 giờ qua
def get_videos_within_24h():
    current_time = datetime.datetime.now(datetime.timezone.utc)
    twenty_four_hours_ago = current_time - datetime.timedelta(hours=24)
    objects = s3.list_objects_v2(Bucket='agri-video', Prefix='24h/')
    video_files = []
    if 'Contents' in objects:
        for obj in objects['Contents']:
            # check mp4 file
            if obj['Key'].endswith('.mp4') and obj['Key'] != '24h/' and obj['LastModified'] >= twenty_four_hours_ago:
                video_files.append(obj['Key'])
    print(video_files)
    return video_files

# Cắt video theo mốc thời gian và tải lên Wasabi
def cut_and_upload_videos(video_files):
    print(video_files)
    # video_files: ['24h/2021-08-25/2021-08-25_00-00-00.mp4', '24h/2021-08-25/2021-08-25_00-05-00.mp4', ...]
    for video_file in video_files:
        # Thời điểm bắt đầu và kết thúc cắt video
        start_time = '00:05:00'  # Bắt đầu từ phút 5
        end_time = '00:10:00'    # Kết thúc ở phút 10

        # Tạo đường dẫn cho video cắt
        output_file = f'cut_videos/cut_{video_file.split("/")[-1]}'

        full_video_path = f'https://s3.ap-southeast-1.wasabisys.com/agri-video/{video_file}'

        # Command FFmpeg để cắt video từ start_time đến end_time
        command = f'ffmpeg -i {full_video_path} -ss {start_time} -to {end_time} -c copy {output_file}'

        # Thực thi lệnh FFmpeg
        subprocess.run(command, shell=True)

        # # Upload video mới lên Wasabi
        # with open(output_file, 'rb') as f:
        #     s3.upload_fileobj(f, 'agri-video', output_file)

        # # Xóa video tạm sau khi upload
        # os.remove(output_file)

        # print(f'Cut and upload {video_file} to Wasabi')


# Xóa video 24h cũ từ Wasabi
def delete_old_videos(video_files):
    for video_file in video_files:
        s3.delete_object(Bucket='your-bucket-name', Key=video_file)

# Chạy quy trình
if __name__ == "__main__":
    videos = get_videos_within_24h()
    if videos:
        cut_and_upload_videos(videos)
        # delete_old_videos(videos)
