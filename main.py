from router import route
from validator import validate
from email_service import send_error_email
from output_writer import save_output
from config import INPUT_FOLDERS

results = []


def process_file(file_path):
    try:
        data = route(file_path)
        errors = validate(data)

        if errors:
            send_error_email(file_path, errors)
            return

        results.append(data)
        save_output(results)

    except Exception as e:
        send_error_email(file_path, [str(e)])


if __name__ == "__main__":
    from watcher import start_watch

    for folder in INPUT_FOLDERS:
        start_watch(folder)