import os
import platform
import subprocess


def build_proto():
    PROJECT_SRC_DIR = os.path.abspath(os.path.join(os.getcwd(), "../../../.."))
    CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
    PROTO_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "proto/source"))
    PROTO_OUT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "proto/generated"))

    os.makedirs(PROTO_OUT_DIR, exist_ok=True)

    for source_file in os.listdir(PROTO_DIR):
        subprocess.call(
            'python3 -m grpc_tools.protoc -I . --python_out=' + PROTO_OUT_DIR + ' --grpc_python_out=' + PROTO_OUT_DIR
            + ' --proto_path =' + PROTO_DIR + ' '
            + os.path.abspath(os.path.join(PROTO_DIR, source_file)), shell=True)

    for generated_file in os.listdir(PROTO_OUT_DIR):
        if platform.system() == "Darwin":
            subprocess.call("sed -i '' 's/^import /from . import /' " + os.path.abspath(
                os.path.join(PROTO_OUT_DIR, generated_file)), shell=True)
        elif platform.system() == "Linux":
            subprocess.call("sed -i 's/^import /from . import /' " + os.path.abspath(
                os.path.join(PROTO_OUT_DIR, generated_file)), shell=True)