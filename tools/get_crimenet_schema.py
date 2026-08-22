import json
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
DATA_DIR = TOOLS_DIR / "data"

def get_json_schema(file_path):
    schema = {}

    def infer_type(value):
        if isinstance(value, dict):
            return {k: infer_type(v) for k, v in value.items()}
        elif isinstance(value, list):
            # Infer type from the first element if list is not empty
            return [infer_type(value[0])] if value else ["unknown"]
        else:
            return type(value).__name__

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

            # Top level may be a list or a dict (crimenet.json is a dict).
            if isinstance(data, list) and len(data) > 0:
                # Sample the first item to infer schema
                schema = infer_type(data[0])
            elif isinstance(data, dict):
                schema = infer_type(data)

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out_path = DATA_DIR / "crimenet_schema.json"
        with open(out_path, 'w') as out:
            json.dump(schema, out, indent=4)
        print(f"Wrote {out_path}")

    except Exception as e:
        print(f"Error processing JSON: {e}")

if __name__ == "__main__":
    get_json_schema(str(TOOLS_DIR.parent / "data" / "crimenet.json"))