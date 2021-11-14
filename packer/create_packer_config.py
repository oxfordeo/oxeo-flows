from pathlib import Path
import json

cloud_config = Path("cloud_config.yaml").read_text()

with Path("packer_empty.json").open() as f:
    packer_config = json.load(f)

packer_config["builders"][0]["metadata"]["user-data"] = cloud_config

with Path("packer.json").open("w") as f:
    json.dump(packer_config, f, indent=4)
