import mysql.connector 
import language_check
import json

f = open('config.json','r')

config = json.loads(f.read())

db = mysql.connector.connect(
    host=config['host'],
    user=config['user'],
    passwd=config['passwd'],
    database=config['database']
)

def get_utterances(corpus_id):
    utterance_cursor = db.cursor(prepared=True)

    utterance_cursor.execute("SELECT * FROM utterance WHERE corpus_id = %s",(str(corpus_id),))

    return utterance_cursor.fetchall()


corpus_cursor = db.cursor(prepared=True)

corpus_cursor.execute("select A.*, B.name as collection from corpus A INNER JOIN collection B ON A.collection_id = B.id WHERE A.name = %s ;",("McMillan",))

for corpus in corpus_cursor.fetchall():
    print(corpus[1])

    if corpus[3].decode("utf-8") == 'Eng-NA':
        lang = 'en-US'
    elif corpus[3].decode("utf-8") == "Eng-UK":
        lang = 'en-UK'
    else:
        exit

    utterances = get_utterances(corpus[0])
    
    print(len(utterances))
    tool = language_check.LanguageTool(lang)

    for utterance in utterances:
        c = db.cursor(prepared=True)
        c.execute("SELECT id, gloss, stem, part_of_speech, token_order, utterance_id, replacement from token where utterance_id = %s",(str(utterance[0]),))
        res = c.fetchall()
        tags = []
        tokens = []
        for w in res:
            word = w[1]
            
            if w[6]:
                word = w[6] 

            tk =" ".join(word.decode("utf-8").split("_"))
            tk = " ".join(tk.split('+'))

            tokens.append(tk)
            # tokens.append(" ".join(w[1].decode("utf-8").split("+")))
        sentence = " ".join(tokens)
        
        st = tool.correct(sentence)

        print(sentence,'>>', st)

        matches = tool.check(sentence)
        for m in matches:
            print(m.ruleId)
            if m.ruleId not in ['UPPERCASE_SENTENCE_START']:
                print(m)

        print('\n')