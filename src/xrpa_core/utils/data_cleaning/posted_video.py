from sqlalchemy import select

from xrpa_core.db.models import DatabaseManager, PostedVideo

dm = DatabaseManager()
with dm.get_session() as session:
    posted_videos = session.execute(select(PostedVideo)).scalars().all()
    for video in posted_videos:
        video_url = video.video_url
        if not video_url:
            raise RuntimeError("Missing video URL for video ID: {video.video_id}")

        video_id = video_url.split("/")[-1]

        if len(video_id) != 19:
            print(f"Invalid video URL: {video.video_url}")
            raise RuntimeError("Invalid video URL")

        video.video_id = video_id
    session.commit()
