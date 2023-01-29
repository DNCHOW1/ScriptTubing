import time
import datetime
import re
import firebase_admin

from google.cloud import storage
from firebase_admin import firestore, credentials

db = None
transcripts = None
client = None
bucket = None
timestampRegex = re.compile(r"<p begin=\"(.*)\" end=\"(.*?)\".*?>(.*?)<\/p>")

def hello_gcs(event, context):
    global client, bucket, db, transcripts, timestampRegex
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    if not client:
        client = storage.Client()
        bucket = client.get_bucket('youtube-subtitles')
    if not db:
        cred = credentials.Certificate("./...")
        app = firebase_admin.initialize_app(cred)
        db = firestore.client()
        transcripts = db.collection(u"transcripts")

    file_blob = storage.Blob(file['name'], bucket)
    download_data = file_blob.download_as_text()

    out = []
    linesSplit = download_data.split("\n")
    for line in linesSplit:
        if len(line) > 1 and line[1] == 'p': # Somewhat fragile, idc
            match = re.search(timestampRegex, line)
            start = match.group(1)
            end = match.group(2)
            text = match.group(3).replace("<br />", " ")
            x = time.strptime(start, "%H:%M:%S.%f")
            startSec = int(datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds())
            out.append((start, end, text, startSec))
    videoId = linesSplit[-4]
    categories = eval(linesSplit[-3])
    channel = linesSplit[-2]

    batchIndex = 0
    with db.batch() as batch_writer:
        for start, end, text, startSec in out:
            docRef = transcripts.document()
            batch_writer.set(docRef, {
                u"start": start,
                u"end": end,
                u"text": text,
                u"start_sec": startSec,
                u"video_id": videoId,
                u"categories": categories,
                u"channel": channel
            })
            batchIndex += 1

            if batchIndex % 500 == 0:
                batch_writer.commit()

        if batchIndex % 500 != 0: batch_writer.commit()
