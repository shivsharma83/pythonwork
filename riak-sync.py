from riakconnect import riakconnect
import sys, argparse, os


def args():
    # Function args - handle command line arguments
    parser = argparse.ArgumentParser(
        description='This script will can be used for multiple purpose to perform operations on riak clusters.')
    parser.add_argument('-s', '--sourceRiak',
                        help='source cluster of Riak to perform operation. values are DEV / QA / LIVE / QAA/ QAB / LIVEA / LIVEB (only Caps)',
                        required=False)
    parser.add_argument('-t', '--targetRiak',
                        help='target cluster of Riak to perform operation. values are DEV / QA / LIVE  / QAA/ QAB / LIVEA / LIVEB (only Caps)',
                        required=True)
    parser.add_argument('-b', '--bucket', help='bucket name of the cluster. e.g. image or rendition', required=True)
    parser.add_argument('-f', '--file', help='file name which consists all the keys for this operation', required=False)
    parser.add_argument('-c', '--command', help='operation that you want to execute. options are DEL or COPY or GET.',
                        required=True)
    arguments = parser.parse_args()
    return arguments

# Fucntion to select right type of riak cluster and return the connection client
def which_riak(riak):

    if riak == 'LIVE':
        RC = riakconnect.live_nodes()
        print("Connection Was successful .....")
        return RC
    elif riak == 'QA':
        RC = riakconnect.qa_nodes()
        print("Connection Was successful .....")
        return RC
    elif riak == 'QAA':
        RC = riakconnect.qaa_nodes()
        print("Connection Was successful .....")
        return RC
    elif riak == 'QAB':
        RC = riakconnect.qab_nodes()
        print("Connection Was successful .....")
        return RC
    elif riak == 'LIVEA':
        RC = riakconnect.livea_nodes()
        print("Connection Was successful .....")
        return RC
    elif riak == 'LIVEB':
        RC = riakconnect.liveb_nodes()
        print("Connection Was successful .....")
        return RC
    else:
        RC = riakconnect.dev_nodes()
        print("Connection Was successful .....")
        return RC


def get_keys(triak, bucket):

    RC = which_riak(triak)
    Bucket = RC.bucket(bucket)
    file_str = '/tmp/'+ str(triak) +'_'+ str(bucket)
    File = open(file_str, 'w')
    allkeys = RC.get_keys(Bucket)
    for key in allkeys:
        #keystr = str(key)[3:-2]
        File.write(str(key).strip() + '\n')


def copy_images_between_riaks_by_file(sriak, triak, bucket, file):
    # Function copy_images_between_riak - this will be used to copy images between two Riak instances, requires bucket

    RC_from = which_riak(sriak)
    RC_to = which_riak(triak)

    sBucket = RC_from.bucket(bucket)
    tBucket = RC_to.bucket(bucket)

    if os.path.isfile(file):
        for Key in open(file):
            if Key.strip() != '':
                object = sBucket.get(Key.strip())
                Data = object.encoded_data
                Type = object.content_type
                stored_data = tBucket.new(key=Key.strip(), content_type=Type, encoded_data=Data)
                stored_data.store()
    else:
        print('File doesnt Exists. Can not proceed the operations. Exiting....')
        sys.exit(0)


def delete_images_from_riak_by_file(triak, bucket, file):
    # Function delete_images_from_riak - this will delete images from a riak's bucket by reading the file, requires bucket

    RC = which_riak(triak)
    Bucket = RC.bucket(bucket)

    if os.path.isfile(file):
        for Key in open(file):
            if Key.strip() != '':
                try:
                    print(Key.strip() + ":" + str(Bucket.get(Key.strip()).exists))
                    object_img = Bucket.get(Key.strip())
                    object_img.delete().exists
                    print(Key.strip() + ":" + str(Bucket.get(Key.strip()).exists))
                except:
                    print(
                        "Deleted the key but couldn't get the proper response from the server:" + str(Bucket.get(key).exists))
                    pass
    else:
        print('File doesnt Exists. Can not proceed the operations. Exiting....')
        sys.exit(0)


def complete_copy(sriak, triak, bucket):
    # Function complete_copy - this will copy everything from one riak to another riak requires bucket

    RC_from = which_riak(sriak)
    RC_to = which_riak(triak)

    sBucket = RC_from.bucket(bucket)
    tBucket = RC_to.bucket(bucket)

    print("Getting All the keys..............")
    allkeys = sBucket.get_keys()
    number_key = 0
    File = open('/tmp/Keys_copying', 'w')

    for Key in allkeys:
        if sBucket.get(str(Key)).exists:
            object = sBucket.get(Key)
            data = object.encoded_data
            Type = object.content_type
            stored_data = tBucket.new(key=Key, content_type=Type, encoded_data=data)
            stored_data.store()


def complete_delete(triak, bucket):
    # Function complete Delete - This function will delete everything from one riak instance, requires bucket

    RC = which_riak(triak)
    Bucket = RC.bucket(bucket)
    print("Getting All the keys..............")
    allkeys = Bucket.get_keys()
    number_key = 0
    File = open('/tmp/Keys_deleted', 'w')

    for Key in allkeys:
        try:
            if Bucket.get(str(Key)).exists:
                object_img = Bucket.get(Key.strip())
                object_img.delete().exists
                File.write(str(Key) + '\n')
                number_key = number_key + 1
        except:
            pass

    print ("total Keys deleted:" + str(number_key))
    File.close()


if __name__ == "__main__":
    arguments = args()

    if arguments.command == "DEL":
        if arguments.targetRiak == 'LIVE':
            print('I Can not execute this operation on LIVE. Sorry!!! Exiting...')
            sys.exit(0)
        else:
            if arguments.file:
                print(
                    "Starting to delete Images from " + arguments.targetRiak + " and Bucket " + arguments.bucket + " by Reading this file keys " + arguments.file)
                delete_images_from_riak_by_file(arguments.targetRiak, arguments.bucket, arguments.file)
            else:
                answer = raw_input('No Correct file provided. Do you want to delete complete bucekt? [YES/NO]')
                if answer != 'YES':
                    print(
                        'Phewww....So you just want to delete the images from File and I cant access that file. Please provide the correct file with right permission. Exiting....')
                    sys.exit(0)
                else:
                    print(
                        "Starting the process to Delete file from " + arguments.targetRiak + " and Bucket " + arguments.bucket + " However, I will wait for 1 min so you can to CTRL+C")
                    #os.sleep(60)
                    complete_delete(arguments.targetRiak, arguments.bucket)

    elif arguments.command == "COPY":
        if arguments.targetRiak == "LIVE":
            print('You Can not copy images to LIVE Riak from QA or LIVE. Existing the program')
            sys.exit(0)
        elif arguments.file:
            if arguments.sourceRiak:
                print(
                    "Starting to copy Images from " + arguments.sourceRiak + " to " + arguments.targetRiak + " For Bucket " + arguments.bucket + " By Reading this file keys " + arguments.file)
                copy_images_between_riaks_by_file(arguments.sourceRiak, arguments.targetRiak, arguments.bucket,
                                                  arguments.file)
            else:
                print(
                    'You have not specefied the Source Riak name, I dont know where to copy these keys from. Exiting....')
                sys.exit(0)
        else:
            if arguments.sourceRiak:
                print("Starting to copy Images from " + arguments.sourceRiak + " to " + arguments.targetRiak)
                complete_copy(arguments.sourceRiak, arguments.targetRiak, arguments.bucket)
            else:
                print('You have not specefied the Source Riak name, I dont know where to copy keys from. Exiting....')
                sys.exit(0)
    elif arguments.command == "GET":
        print("Getting Keys and will save them into file /tmp/" + arguments.targetRiak + '_' + arguments.bucket)
        get_keys(arguments.targetRiak, arguments.bucket)
    else:
        print('Couldn\'t understand what you wanted to do. Please refer to help')
        sys.exit(0)

