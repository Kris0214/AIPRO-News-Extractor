import paramiko

# Define SFTP connection parameters
hostname = '10.81.70.11'
port = 22
username = 'datascientist.sec'
password = 'Fubon16905'

# Create an SSH client
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# Connect to the SFTP server
ssh_client.connect(hostname, port, username, password)

# Create an SFTP session
sftp = ssh_client.open_sftp()


local_file = 'C:\\Users\AI-Pro\\AIPRO-投顧報告與新聞\\requirements.txt'
remote_file = '/MSGHDataCenter/AIPRO/requirements.txt'
sftp.put(local_file, remote_file)

directory = '/MSGHDataCenter/AIPRO'
files = sftp.listdir(directory)

print(files)

sftp.close()
ssh_client.close()