from openai import AzureOpenAI

def call_AOAI_api_prompt(client, content, schema, prompt, response_format={"type": "json_object"}):
    response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": content,
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        max_tokens=5000,
        temperature=0.1,
        top_p=1.0,
        model = "gpt-4o"
    )

    response_txt = response.choices[0].message.content
    return response_txt