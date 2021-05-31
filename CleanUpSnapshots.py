import boto3
import traceback
import datetime

ec2 = boto3.client('ec2')
# sts -- AWS Security Token Service
sts = boto3.client('sts')

def lambda_handler(event, context):
    
    try:
        # Step 1: Obter parâmetro que chama o programa
        parameters = get_parameters_from_event(event)
        days_of_snapshots_retention = parameters['days_of_snapshots_retention']
    
        # Step 2: Get AWS account ID
        account_id = get_account_id()
    
        # Step 3: Obter todos os snapshots
        snapshots = get_snapshots(days_of_snapshots_retention, account_id)
    
        # Step 4: Delete snapshots
        for snapshot in snapshots:
            try:
                delete_snapshot(snapshot)
                
            except Exception as e:
                displayException(e)

    except Exception as e:
            displayException(e)
            # traceback.print_exc()


def get_parameters_from_event(event):
    # Cria um dicionário vazio
    parameters = {}
    
    if isinstance(event, dict):

        if 'days_of_snapshots_retention' in event.keys():

            days_of_snapshots_retention = event.get('days_of_snapshots_retention')

            if days_of_snapshots_retention is not None and type(days_of_snapshots_retention) is int:
                
                if days_of_snapshots_retention > 0:
                    parameters['days_of_snapshots_retention'] = days_of_snapshots_retention
                    print ("days_of_snapshots_retention is %d" % days_of_snapshots_retention)
                else:
                    raise Exception("Parameter days_of_snapshots_retention is invalid (not greater than 0). Program exits without doing anything.")
            else:
                raise Exception("Parameter days_of_snapshots_retention is invalid (None type or not an integer). Program exits without doing anything.")
        else:
            raise Exception("No days_of_snapshots_retention in the event.")
    else:
        raise Exception("event is not dictionary data type.")
        
    return parameters

def get_account_id():
    account_id = sts.get_caller_identity().get('Account')
    # print ("account_id: %s" % account_id)
    return account_id

def get_snapshots(days_of_snapshots_retention, account_id):
    
    snapshots_after_filter = ec2.describe_snapshots(
        Filters=[
            {'Name': 'owner-id', 'Values': [account_id]},
            {'Name': 'tag-key', 'Values': ['Auto-CleanUp-Enabled']}
        ]
    ).get(
        'Snapshots', []
    )
    
    # print ("snapshots_after_filter %d" % len(snapshots_after_filter))
    
    snapshots_match_datetime = [
        snapshot 
        for snapshot in snapshots_after_filter 
        if snapshot['StartTime'] < (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_of_snapshots_retention))
        ]

    print ("Found %d snapshots that need to be cleaned up" %  len(snapshots_match_datetime))

    return snapshots_match_datetime

def delete_snapshot(snapshot):
    ec2.delete_snapshot(
        SnapshotId=snapshot['SnapshotId'],
        #DryRun=True
    )
    print (snapshot['SnapshotId'] + " was deleted successfully")

def displayException(exception):
    exception_type = exception.__class__.__name__ 
    exception_message = str(exception) 

    print("Exception type: %s; Exception message: %s;" % (exception_type, exception_message))
