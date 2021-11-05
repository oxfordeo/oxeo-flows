# prefect

https://docs.prefect.io/core/getting_started/install.html

## Cloud
Log in at [Prefect Cloud](https://cloud.prefect.io/) and get an API key from the user account page (different from an agent/project key).

## Install
```
workon pf
mkvirtualenv prefect && workon prefect
pip install requests pandas prefect[viz,google,redis,gcp]
```

## Getting started
Tell core that we're working with Cloud and authenticate:
```
prefect backend cloud
prefect auth login --key "key-from-prefect.io"
```

Then run the Flow files to register the Flow with the cloud:
```
python flow_file.py
```

Then start the agent wherever you want that run:
```
prefect agent local start
```

And control from the web UI!
