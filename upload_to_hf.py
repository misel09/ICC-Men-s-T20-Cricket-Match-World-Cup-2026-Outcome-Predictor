#!/usr/bin/env python3
"""
Upload cricket prediction files to Hugging Face Space
"""

import os
from huggingface_hub import HfApi, login
from pathlib import Path

def upload_to_space(space_name):
    """
    Upload all necessary files to the Hugging Face Space
    """
    api = HfApi()

    # Files to upload
    files_to_upload = [
        "gradio_app.py",
        "requirements.txt",
        "README.md"
    ]

    # Upload individual files
    for file_path in files_to_upload:
        if os.path.exists(file_path):
            print(f"Uploading {file_path}...")
            api.upload_file(
                path_or_fileobj=file_path,
                path_in_repo=file_path,
                repo_id=space_name,
                repo_type="space"
            )
        else:
            print(f"Warning: {file_path} not found")

    # Upload models directory
    models_dir = "models"
    if os.path.exists(models_dir):
        for model_file in os.listdir(models_dir):
            model_path = os.path.join(models_dir, model_file)
            if os.path.isfile(model_path):
                print(f"Uploading {model_path}...")
                api.upload_file(
                    path_or_fileobj=model_path,
                    path_in_repo=f"models/{model_file}",
                    repo_id=space_name,
                    repo_type="space"
                )

    print("✅ All files uploaded successfully!")
    print(f"🚀 Your app will be available at: https://huggingface.co/spaces/{space_name}")

if __name__ == "__main__":
    # Replace with your actual space name
    space_name = input("Enter your Hugging Face Space name (format: username/spacename): ")

    # Check if logged in
    try:
        from huggingface_hub import whoami
        user = whoami()
        print(f"Logged in as: {user['name']}")
    except:
        print("Please login first with: huggingface-cli login")
        exit(1)

    upload_to_space(space_name)