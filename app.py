from flask import Flask, request, jsonify, render_template
import openai
import json  # Import the JSON module
import pandas as pd
import os
import redis

from football_dataset_groupings import group_by_country, group_by_tournament, group_by_year, group_by_team

app = Flask(__name__)


# Initialize Redis client
redis_client = redis.from_url(os.environ.get('REDIS_URL'))

# Set your OpenAI API key
openai.api_key = os.environ.get('OPENAI_API_KEY')

# Initialize responses_cache
responses_cache = {}

# Load and store the DataFrame on startup
df = pd.read_csv('static/results.csv')

# Function to clear Redis cache
def clear_redis_cache():
    redis_client.flushdb()
    print("Redis cache cleared.")

    # Clear the Redis cache on startup
    clear_redis_cache()


@app.route('/group-data/<groupByValue>', methods=['GET'])
def group_data(groupByValue):
    cache_key = f"group_data_{groupByValue}"

    # Check if the response for this groupByValue is already in the cache
    if cache_key in responses_cache:
        return jsonify(responses_cache[cache_key])

    # Check if the response for this groupByValue is already in the cache
    cached_response = redis_client.get(cache_key)
    #if cached_response:
     #   return jsonify(json.loads(cached_response.decode('utf-8')))

    # If not in cache, process data
    if groupByValue == 'team':
        response = group_by_team(df)
    elif groupByValue == 'tournament':
        response = group_by_tournament(df)
    elif groupByValue == 'year':
        response = group_by_year(df)

    elif groupByValue == 'country':
        response = group_by_country(df)
    else:
        return jsonify({'error': 'Invalid group by value'}), 400

    #print(response.shape)
    # Convert NaN values to None for JSON serialization
    response = response.applymap(lambda x: "none" if pd.isna(x) else x)
    # Convert to a list of dictionaries for JSON serialization, if not already
    response = response.to_dict(orient='records')

    # Store the response in the cache
   # responses_cache[cache_key] = response
    # Cache the response in Redis
    redis_client.set(cache_key, json.dumps(response))
    print(f"Data cached for key: {cache_key}")
    # Optionally, you can save the cache to a file here as well
    # save_responses_to_json()

    return jsonify(response)

# Function to save responses_cache to JSON
def save_responses_to_json():
    with open('responses_cache.json', 'w') as json_file:
        json.dump(responses_cache, json_file)

# Function to load responses_cache from JSON
def load_responses_from_json():
    try:
        with open('responses_cache.json', 'r') as json_file:
            loaded_cache = json.load(json_file)
            responses_cache.update(loaded_cache)  # Update the existing cache with loaded data
    except FileNotFoundError:
        # Handle file not found error, or initialize responses_cache if the file doesn't exist
        responses_cache.clear()

@app.route('/visualization', methods=['GET', 'POST'])
def show_visualization():
    return render_template('football_data_viz_00_8_3.html')

@app.route('/chatgpt', methods=['POST'])
def get_gpt_data():
    try:
        data = request.json
        bubble_data = data['bubbleData']
        bubble_type = data['bubbleType']

        # Check if response for this bubble_data and bubble_type is already cached
        cache_key = f"{bubble_type}_{bubble_data}"
        cached_response = redis_client.get(cache_key)
        if cached_response:
            return jsonify(json.loads(cached_response.decode('utf-8')))

        prompt = construct_prompt(bubble_data, bubble_type)

        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]

        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=messages
        )

        response_data = {
            "id": response['id'],
            "content": response['choices'][0]['message']['content']
        }

        # Store the response in Redis cache
        redis_client.set(cache_key, json.dumps(response_data))

        return jsonify(response_data)

    except Exception as e:
        error_message = f"Error: {e}"
        return jsonify({"error": error_message})


def construct_prompt(bubble_data, bubble_type):
    # Create a prompt based on the type of bubble and its data
    if bubble_type == 'team':
        prompt = f"Please, provide some interesting football related facts about national {bubble_type} of {bubble_data}"
    elif bubble_type == 'tournament':
        prompt = f"Please, provide some interesting football related facts about {bubble_type} in context of {bubble_data}"
    elif bubble_type == 'year':
        prompt = f"Please, provide some interesting football related facts about {bubble_data}"
    elif bubble_type == 'country':
        prompt = f"Please, provide some interesting football related facts about {bubble_type} as a country that is a hoster of football matches with the following statics: {bubble_data}"

    print(prompt)  # Print the prompt to the console
    return prompt

if __name__ == '__main__':
    # Load cached responses from JSON when the script starts up
    load_responses_from_json()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)  # Turn off debug mode for production

