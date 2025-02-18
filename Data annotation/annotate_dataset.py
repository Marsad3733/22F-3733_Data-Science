import openai
import csv
import os
import json
import asyncio
import aiohttp


openai.api_key = "sk-681422df2a2d4488adb036f87ddbe913"

INPUT_JSON_FILE = "C:/NeurIPS_papers/metadata.json"
OUTPUT_CSV_FILE = "C:/NeurIPS_papers/annotated_metadata.csv"

ANNOTATION_CATEGORIES = [
    "Deep Learning",
    "Computer Vision",
    "Reinforcement Learning",
    "Natural Language Processing",
    "Optimization"
]


async def annotate_with_gemini(title, abstract):
    prompt = (
        f"The following research paper has a title and an abstract. "
        f"Classify the paper into one of these categories: {', '.join(ANNOTATION_CATEGORIES)}.\n\n"
        f"Title: {title}\n"
        f"Abstract: {abstract}\n\n"
        f"Category:"
    )

    try:
        response = await openai.ChatCompletion.acreate(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0
        )

        # Extract the category from the response
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[ERROR] LLM Annotation failed: {e}")
        return "Uncategorized"



async def annotate_dataset():
    
    if not os.path.exists(INPUT_JSON_FILE):
        print(f"[ERROR] Input JSON file not found: {INPUT_JSON_FILE}")
        return

    # Load metadata from JSON
    with open(INPUT_JSON_FILE, "r", encoding="utf-8") as file:
        try:
            papers = json.load(file)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to load JSON data: {e}")
            return

    
    with open(OUTPUT_CSV_FILE, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["year", "title", "authors", "abstract", "pdf_url", "category"])
        writer.writeheader()

        async with aiohttp.ClientSession() as session:
            for paper in papers:
                title = paper.get("title", "Unknown Title")
                abstract = paper.get("abstract", "Abstract not available")
                category = await annotate_with_gemini(title, abstract)

                paper["category"] = category
                writer.writerow(paper)
                print(f"[INFO] Annotated: {title} -> {category}")

    print(f"[SUCCESS] Annotated data saved to {OUTPUT_CSV_FILE}")


if __name__ == "__main__":
    asyncio.run(annotate_dataset())
