echo '
<ssh key here>
' > ~/.ssh/id_ed

echo '
<gcp-json-auth-here>
' > token.json

echo '
<requirements.txt-here>
' > requirements.txt

export PREFECT__CLOUD__API_KEY="<prefect-api-key-here>"

apt-get update
apt-get install -y software-properties-common openssh-client git \
  apt-transport-https ca-certificates gnupg curl
add-apt-repository -y ppa:deadsnakes/ppa
apt-get install -y python3.9 python3.9-distutils python3-dev python3-pip

rm /usr/bin/python /usr/bin/python3
ln -s /usr/bin/python3.9 /usr/bin/python3
ln -s /usr/bin/python3 /usr/bin/python

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
  tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
  apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
apt-get update
apt-get install -y google-cloud-sdk

chmod 400 ~/.ssh/id_ed
eval $(ssh-agent)
ssh-add ~/.ssh/id_ed
ssh-keyscan -H github.com >> ~/.ssh/ssh_known_hosts

python3 -m pip install --upgrade setuptools pip distlib
python3 -m pip3 install -r requirements.txt
