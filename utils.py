import boto3
import logging
import re
import time
import json
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)



def upload_and_retrieve(ACCESS_ID, ACCESS_KEY, bucket_name, file, vocabulary=None):
    """
    Uploads a file to an AWS S3 bucket, transcribes the audio file into text using AWS Transcribe service, and returns the transcribed text.
    
    Parameters:
    ACCESS_ID (str): AWS access key ID.
    ACCESS_KEY (str): AWS secret access key.
    bucket_name (str): Name of the S3 bucket.
    file (str): Path of the file to be transcribed.
    
    Returns:
    str: The transcribed text of the audio file.
    """
    # Create a session with AWS credentials
    session = boto3.Session(
        aws_access_key_id=ACCESS_ID,
        aws_secret_access_key=ACCESS_KEY
    )
    
    # Create an S3 client
    s3_client = session.client('s3')
    
    # Create an S3 bucket in eu-west-2 region if it doesn't exist
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            })
    except Exception as e:
        # Log a warning if the bucket already exists
        logging.warning(re.findall(r'\((\w+)\)', str(e))[0])

    # Upload the file to the S3 bucket
    s3_client.upload_file(file, bucket_name, file.split("\\")[-1])
    
    # Create a transcribe client
    transcribe_client = boto3.client('transcribe', region_name='eu-west-2',  aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)
    
    settings= {}
    if vocabulary is not None:
        settings['VocabularyName'] = vocabulary
        logging.debug(settings)
    else:
        vocabulary = {}
    
    # Start the transcription job
    response = transcribe_client.start_transcription_job(
        TranscriptionJobName=f'transcribing_{file.split(".")[0]}_{str(int(time.time()))}',
        LanguageCode='en-US',
        MediaFormat='flac',
        Settings={},
        Media={
            'MediaFileUri': f"s3://{bucket_name}/{file}"
        },
        OutputBucketName=bucket_name,
        OutputKey=f"{file.split('.')[0]}.txt"
    )
    
    # Download the transcribed text file from S3 bucket
    s3 = boto3.resource('s3',  aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)
    while True:
        try:      
            s3.Object(bucket_name, f"{file.split('.')[0]}.txt").download_file(f"{file.split('.')[0]}.txt")
            break
        except:
            # Log a message if the file is not yet available
            logging.info("Waiting for transcription...")
            time.sleep(1)
    
    # Read the transcribed text from the file
    with open(f"{file.split('.')[0]}.txt") as f:
        out = ""
        for line in f:
            out += line
    
    # Parse the transcribed text from the JSON response
    return json.loads(out)["results"]["transcripts"][0]['transcript']

def create_vocabulary(
        vocabulary_name, language_code, transcribe_client,
        phrases=None, table_uri=None):
    """
    Creates a custom vocabulary that can be used to improve the accuracy of
    transcription jobs. This function returns as soon as the vocabulary processing
    is started. Call get_vocabulary to get the current status of the vocabulary.
    The vocabulary is ready to use when its status is 'READY'.

    :param vocabulary_name: The name of the custom vocabulary.
    :param language_code: The language code of the vocabulary.
                          For example, en-US or nl-NL.
    :param transcribe_client: The Boto3 Transcribe client.
    :param phrases: A list of comma-separated phrases to include in the vocabulary.
    :param table_uri: A table of phrases and pronunciation hints to include in the
                      vocabulary.
    :return: Information about the newly created vocabulary.
    """
    try:
        vocab_args = {'VocabularyName': vocabulary_name, 'LanguageCode': language_code}
        if phrases is not None:
            vocab_args['Phrases'] = phrases
        elif table_uri is not None:
            vocab_args['VocabularyFileUri'] = table_uri
        response = transcribe_client.create_vocabulary(**vocab_args)
        logging.info("Created custom vocabulary %s.", response['VocabularyName'])
    except:
        logging.info("Couldn't create custom vocabulary %s.", vocabulary_name)
        raise
    else:
        
        return response
    
def wait_for_vocabulary(transcribe_client, vocabulary_name):
    while True:
        response = transcribe_client.get_vocabulary(VocabularyName=vocabulary_name)
        logging.info(response['VocabularyState'])
        if response['VocabularyState'] == 'READY':
            break
        elif response['VocabularyState']== 'FAILED':
            logging.critical(response)
            logging.critical(f"Vocabulary {vocabulary_name} {response['VocabularyState']}")
            raise RuntimeError
                
        elif response['VocabularyState'] == 'PENDING':
            time.sleep(10)
        
#import my_secrets as sct
#bucket_name = "test-bucket-dawid-kohnke-7"
#vocabulary_name = "vocabulary_test_18"
#file = "test_final.flac"
#phrases_0 = ('Mackie','Cathy','Blenheim','U.S.','U.K.','Chequers','LBJ')
#transcribe_client = boto3.client('transcribe', region_name='eu-west-2',  aws_access_key_id=sct.ACCESS_ID, aws_secret_access_key= sct.ACCESS_KEY)
#print(create_vocabulary(vocabulary_name, 'en-US', transcribe_client, phrases_0, f's3://{bucket_name}/{vocabulary_name}.txt'))
#wait_for_vocabulary(transcribe_client, vocabulary_name)
#print(upload_and_retrieve(sct.ACCESS_ID, sct.ACCESS_KEY, bucket_name = bucket_name, file = file, vocabulary=vocabulary_name))
