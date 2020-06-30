import re
import pandas as pd
from tqdm import tqdm
from glob import glob
from datetime import datetime


paths = glob('../data/maildir/*/_sent_mail/*')
paths = sorted(paths)


def prepare_datetime(s):
    tmp = datetime.strptime(
        s[5:-12],
        "%d %b %Y %H:%M:%S"
    )
    return tmp


def only_email(s):
    mail = re.compile('[\w\.-]+@[\w\.-]+\.\w+')
    mails = re.findall(mail, s)
    return mails


def get_text(lines):
    idx = 0
    for i, line in enumerate(lines):
        if not len(line.strip()):
            idx = i
            break
    text = "\n".join(map(lambda x: x.strip(), lines[idx:]))
    try:
        idx = text.index('- Forwarded')
        if idx > -1:
            text = text[:idx].strip(' -')
    except:
        pass
    return text


def make_dataset(paths=paths):
    d = {}
    data = []

    for p in tqdm(paths):
        with open(p, 'r', encoding='us-ascii') as f:
            t = f.readlines()
            fr = only_email(t[2])[0]
            to = set(only_email(t[3]))
            subject = t[4].replace('Subject:', '').strip()
            date = prepare_datetime(t[1].replace("Date:", '').strip())
            text = get_text(t).strip()

            for recipient in to:
                d = {
                    'date': date,
                    'subject': subject,
                    'from': fr,
                    'to': recipient,
                    'text': text,
                    'path': p,
                }
                data.append(d)

    df = pd.DataFrame(data)
    df.sort_values(by=['date'], inplace=True)
    return df

