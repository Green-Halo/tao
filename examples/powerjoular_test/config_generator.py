# config_generator.py
import json
import random

# Generate random range for prime number testing
config_data = {"start": random.randint(1, 100), "end": random.randint(101, 1000)}

# Save configuration to a JSON file
with open("config.json", "w") as config_file:
    json.dump(config_data, config_file)

print("Configuration file generated:", config_data)
