import json
import os
import pandas as pd


def load_hyperparameters():
    hyperparameters_path = '/opt/ml/input/config/hyperparameters.json'
    with open(hyperparameters_path) as f:
        hyperparameters = json.load(f)
    return hyperparameters

def load_input_data_config():
    input_data_config_path = '/opt/ml/input/config/inputdataconfig.json'
    with open(input_data_config_path) as f:
        input_data_config = json.load(f)
    return input_data_config

def read_data_from_channel(channel_name, input_mode='File'):
    data_dir = f'/opt/ml/input/data/{channel_name}'
    if input_mode == 'File':
        data_files = [os.path.join(data_dir, file) for file in os.listdir(data_dir)]
        if data_files:
            print(f"Reading data from {data_files[0]}")
            # Check if the data is a CSV or JSON file
            if data_files[0].endswith('.csv'):
                data = pd.read_csv(data_files[0])
            elif data_files[0].endswith('.json'):
                data = pd.read_json(data_files[0])
            else:
                raise ValueError("Unsupported file format")
            data_head = data.head()
            print(data_head)
            return data_head
        else:
            raise ValueError(f"No data files found in {data_dir}")
    elif input_mode == 'Pipe':
        # Implement reading from Pipe mode if necessary
        raise NotImplementedError("Pipe mode is not implemented in this example")

def save_output(hyperparameters, input_data_config, data_heads):
    output_dir = '/opt/ml/output/data'
    os.makedirs(output_dir, exist_ok=True)

    hyperparameters_path = os.path.join(output_dir, 'hyperparameters.json')
    with open(hyperparameters_path, 'w') as f:
        json.dump(hyperparameters, f, indent=4)
    
    input_data_config_path = os.path.join(output_dir, 'inputdataconfig.json')
    with open(input_data_config_path, 'w') as f:
        json.dump(input_data_config, f, indent=4)
    
    for channel_name, data_head in data_heads.items():
        data_head_path = os.path.join(output_dir, f'{channel_name}_head.csv')
        data_head.to_csv(data_head_path, index=False)

def main():
    # Load hyperparameters
    hyperparameters = load_hyperparameters()
    print("Hyperparameters:")
    print(hyperparameters)

    # Load input data configuration
    input_data_config = load_input_data_config()
    print("Input Data Config:")
    print(input_data_config)

    # Read and print the head of the data for each channel and collect data heads
    data_heads = {}
    for channel_name, config in input_data_config.items():
        print(f"\nChannel: {channel_name}")
        data_head = read_data_from_channel(channel_name, config.get("TrainingInputMode", "File"))
        data_heads[channel_name] = data_head

    # Save the output information
    save_output(hyperparameters, input_data_config, data_heads)
    print("\nOutput files saved")

if __name__ == "__main__":
    import sys
    command_line = ' '.join(sys.argv)
    print(f"Command line arguments: {command_line}")
    main()
