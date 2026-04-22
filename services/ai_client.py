from flask import current_app


def generate_structured_text(prompt, system_message="Return ONLY valid JSON."):
    api_key = current_app.config.get("AI_SERVICE_KEY")
    model = current_app.config.get("AI_TEXT_MODEL")
    if not api_key or not model:
        raise RuntimeError("Remote text generation is not configured")

    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt},
        ],
    )
    return response.choices[0].message.content.strip()
