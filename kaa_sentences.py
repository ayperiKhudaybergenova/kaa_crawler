
import os, re, pandas as pd, asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from huggingface_hub import HfApi
from kaalin.converter import cyrillic2latin


api_id = int(os.environ.get("TG_API_ID"))
api_hash = os.environ.get("TG_API_HASH")
phone = os.environ.get("TG_PHONE")
hf_token = os.environ.get("HF_TOKEN")
repo_id = "Ayperi/kaa_sentences"


SESSION_FILE = "telegram_session.txt"


async def create_session_if_missing():
    if not os.path.exists(SESSION_FILE):
        async with TelegramClient(StringSession(), api_id, api_hash) as client:
            await client.start(phone=phone)
            session_str = client.session.save()
            with open(SESSION_FILE, "w") as f:
                f.write(session_str)
            print("✅ Session saved to telegram_session.txt")
    else:
        print("✅ Session file exists, skipping login.")


def clean_sentence(sentence):
    if not sentence:
        return None

    blocked_words = ["nasiyatuz", "instagram", "telegram", "youtube"]
    for w in blocked_words:
        if w.lower() in sentence.lower():
            return None

    if re.search(r"(https?|www\.|\.com|\.uz|\.ru|\.org|@)", sentence.lower()):
        return None


    sentence = re.sub(r"[^\w\s.,!?-]", "", sentence)


    sentence = re.sub(r"\s+", " ", sentence).strip()

    if len(sentence.split()) < 3:
        return None


    return cyrillic2latin(sentence)


async def fetch_messages(session_str, channel_username, limit=250):
    async with TelegramClient(StringSession(session_str), api_id, api_hash) as client:
        await client.connect()
        messages = []
        async for msg in client.iter_messages(channel_username, reverse=True):
            if msg.message:
                cleaned = clean_sentence(msg.message)
                if cleaned:
                    messages.append(cleaned)
        return messages[-limit:]


async def main():
    await create_session_if_missing()
    with open(SESSION_FILE, "r") as f:
        session_str = f.read().strip()

    channels = ["nasiyatuz"]
    all_sentences = []

    for ch in channels:
        print(f"Fetching from {ch}...")
        try:
            sentences = await fetch_messages(session_str, ch)
            all_sentences.extend(sentences)
        except Exception as e:
            print(f"Error fetching {ch}: {e}")

    print(f"Total cleaned sentences: {len(all_sentences)}")


    df = pd.DataFrame(all_sentences, columns=["sentence"])
    text_file = "telegram_sentences.txt"
    df["sentence"].to_csv(text_file, index=False, header=False, encoding="utf-8")


    print("Uploading to Hugging Face...")
    api = HfApi()
    api.upload_file(
        path_or_fileobj=text_file,
        path_in_repo="telegram_sentences.txt",
        repo_id=repo_id,
        repo_type="dataset",
        commit_message="Add clean Latin Karakalpak sentences",
        token=hf_token,
    )

    print(f"✅ Uploaded {len(df)} sentences to Hugging Face!")


if __name__ == "__main__":
    asyncio.run(main())
