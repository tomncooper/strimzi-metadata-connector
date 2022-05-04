from sys import argv, exit
from subprocess import run, CalledProcessError
from pathlib import Path
""" Sets up a 3 node Kafka Cluster:

    1 - The path to the Kafka distribution folder to use
    2 - Path to the config folder with the server.properties files for each broker
"""

kafka_root_folder = Path(argv[1])
if not kafka_root_folder.is_dir():
    print(f"{argv[1]} is not a folder")
    exit(1)
else:
    print(f"Setting up KRaft cluster using Kafka: {kafka_root_folder}")
    ks_path = Path(kafka_root_folder,"bin/kafka-storage.sh")

config_root_folder = Path(argv[2])
if not config_root_folder.is_dir():
    print(f"{argv[2]} is not a folder")
    exit(1)
else:
    print(f"Using configs from: {config_root_folder}")

cluster_id = "Tgmpcy6jSt6oCLMa4kmhRA"

for i in range(3):

    print(f"Formatting storage for broker {i+1} ({cluster_id})")

    config_path = Path(config_root_folder, f"server{i+1}.properties")

    format_command = [str(ks_path), "format", "--config", str(config_path), "--cluster-id", cluster_id]

    format_result = run(format_command, capture_output=True)

    try:
        format_result.check_returncode()
    except CalledProcessError as cpe:
        print("Error formatting storage:")
        print(cpe.stderr)

print("Formatting complete")

