import subprocess
import os


if __name__ == "__main__":
    service_account = os.environ["service_account"]
    shell_param = {"shell": False} if "/" in os.sep else {"shell": True}
    paths = os.getcwd().split(os.sep)
    app_name = f"{paths[-2]}-{paths[-1]}"
    print(app_name)
    project = "medaka-iot"
    build_cmd = f"gcloud builds submit --tag gcr.io/{project}/{app_name}"
    subprocess.call(build_cmd.split(), **shell_param)
    # --no-allow-unauthenticated or --allow-unauthenticated
    deploy_cmd = f"""gcloud run deploy {app_name} \
                --image gcr.io/{project}/{app_name} \
                --platform=managed \
                --no-allow-unauthenticated \
                --region asia-northeast1 \
                --memory 256Mi \
                --service-account={service_account}
                --set-env-vars=service_account={service_account}
                """
    subprocess.call(deploy_cmd.split(), **shell_param)
