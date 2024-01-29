# ensign-shared

## Python version used

python version 3.11.6

## Mount SMB shared folder

Install cfis to mount (SMB)

```bash
sudo apt install cifs-utils
```

Create and mount the smb shared folder

```bash
sudo mkdir /mnt/<FOLDER_NAME>
sudo mount -t cifs -o username=<USERNAME>,password=<PASSWORD> //<SERVER_IP>/<SHARE_NAME> /mnt/<FOLDER_NAME>
```
