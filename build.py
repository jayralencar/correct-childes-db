import mysql.connector 
import language_check
import json
import os
import xml.etree.ElementTree as ET
import re

f = open('config.json','r')

config = json.loads(f.read())


def connect():
    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        passwd=config['passwd'],
        database=config['database']
    )

db = connect()

ns = "{http://www.talkbank.org/ns/talkbank}"

insert_cursor = db.cursor()

punctuation_mapping = {
    'q' : '?',
    'e' : '!',
    'trail off':'.',
    'p': '.',
    'quotation next line' : ':',
    'interruption':'.',
    'trail off question':'?',
    'self interruption': '.',
    'quotation precedes':':',
    'self interruption question':'?',
    'interruption question':'?'
}

a = db.cursor()

a.execute("set global max_prepared_stmt_count = 100000;")

db.commit()

a.close()

def get_node_name(node):
    matchs = re.findall('[a-zA-Z]+(?![^{]*\})',node)
    if len(matchs) > 0:
        return matchs[0]
    return None

def get_participant(id):
    p_cursor = db.cursor(prepared=True)

    p_cursor.execute("SELECT * FROM participant WHERE id = %s",(id,))

    res = p_cursor.fetchone()
    # p_cursor.close()
    return res

def insert(entity, data):
    sql = "INSERT INTO "+entity+" ("+(', '.join(data.keys()))+") VALUES ("+(', '.join('%s' for a in range(len(data.keys()))))+");"
    values = tuple(data.values())

    # print(sql)
    # print(values)
    i_cursor = db.cursor(prepared=True)
    i_cursor.execute(sql,values)

    db.commit()

    return i_cursor.lastrowid
    # i_cursor.close()

def get_word_node(node):
    if get_node_name(node.tag) == 'w':
        return node
    


j = 0
for r,d,f in os.walk('./corpora'): #Percorre corpora
    path = r.split('/')
    if len(path) > 2:
        dirname = path[2]
        print(dirname)
        if dirname in ['Wells']: # Escolhe um corpus
            corpus = {}
            corpus_cursor = db.cursor(prepared=True)

            corpus_cursor.execute("SELECT * FROM corpus where name = %s",(dirname,))

            corpora = corpus_cursor.fetchall()

            if len(corpora) > 0:
                corpus = {
                    'id' : corpora[0][0],
                    'name' : corpora[0][1].decode("utf-8")
                }
                
                for filename in f: # percorre transcritos
                    print('\t'+r+"/"+filename)
                    file_name = r+"/"+filename
                    transcript_cursor = db.cursor(prepared=True)
                    print(file_name, corpus['id'])
                    transcript_cursor.execute("SELECT id, filename, finished FROM transcript_jap WHERE filename =%s AND corpus_id = %s",(file_name, corpus['id'],))
                    res = transcript_cursor.fetchall()
                    # transcript_cursor.close()
                    transcript = {}

                    if len(res) > 0:
                        transcript = {
                            "id" : res[0][0],
                            "filename" : res[0][1],
                            "finished":res[0][2]
                        }

                    else:
                        transcript_id = insert('transcript_jap',{
                            'filename':file_name,
                            'corpus_id': corpus['id']
                        })
                        transcript = {
                            "id" : transcript_id,
                            "filename":file_name,
                            'finished' : 0
                        }

                    if transcript['finished'] == 0: # se o transcrito não foi registrado no banco ainda.
                    # if  "./corpora/Bates/Free20/amy.xml" == r+"/"+filename:
                        tree = ET.parse(r+"/"+filename)
                        root = tree.getroot()
                        participants = {}
                        for child in root:
                            # Participants
                            
                            if get_node_name(child.tag) =="Participants":
                                # identify target_child
                                participant_cursor = db.cursor(prepared=True)
                                for participant in child:
                                    if participant.attrib['id'] == 'CHI':
                                        participant_cursor.execute("SELECT * FROM participant WHERE corpus_id = %s AND name = %s AND code = %s AND role = %s", (corpus['id'],participant.attrib['name'],participant.attrib['id'],participant.attrib['role'], ))
                                        ps = participant_cursor.fetchall()
                                        participants[participant.attrib['id']] = ps[0][0] 
                                
                                for participant in child:
                                    # print(participant.attrib)
                                    
                                    if participant.attrib['id'] == 'CHI':
                                        participant_cursor.execute("SELECT * FROM participant WHERE corpus_id = %s AND name = %s AND code = %s AND role = %s", (corpus['id'],participant.attrib['name'],participant.attrib['id'],participant.attrib['role'], ))
                                        ps = participant_cursor.fetchall()
                                        participants[participant.attrib['id']] = ps[0][0]
                                        # participant_cursor.close()
                                    elif 'CHI' in participants:
                                        participant_cursor.execute("SELECT * FROM participant WHERE corpus_id = %s AND target_child_id = %s AND code = %s AND role = %s", (corpus['id'],participants['CHI'],participant.attrib['id'],participant.attrib['role'], ))
                                        ps = participant_cursor.fetchall()
                                        if len(ps) > 0:
                                            participants[participant.attrib['id']] = ps[0][0]
                                        else:
                                            participant_cursor.execute("SELECT * FROM participant WHERE corpus_id = %s AND code = %s AND role = %s", (corpus['id'],participant.attrib['id'],participant.attrib['role'], ))
                                            ps = participant_cursor.fetchall()
                                            if len(ps) > 0:
                                                participants[participant.attrib['id']] = ps[0][0]
                                        # participant_cursor.close()
                                print(participants) 

                            if get_node_name(child.tag) =="u": # utterances
                                utterance = {
                                    "speaker_id": participants[child.attrib['who']],
                                    'order': int(child.attrib['uID'].split('u')[1]),
                                    'corpus_id': corpus['id']
                                }
                                print("FILE: "+file_name)
                                print("Utterance: "+str(utterance['order']))
                                
                                speaker = get_participant(utterance['speaker_id'])

                                target_child_id = None
                                target_child_age = None
                                target_child_name = None
                                target_child_sex = None

                                if speaker[13] != None:
                                    tc = get_participant(speaker[13])
                                    target_child_id = speaker[13]
                                    target_child_age = tc[12]
                                    target_child_name = tc[2]
                                    target_child_sex = tc[6]
                                
                                utest_cursor = db.cursor(prepared=True)
                                utest_cursor.execute("SELECT id, length FROM utterance_jap WHERE speaker_id = %s AND `order` = %s AND corpus_id = %s AND transcript_id = %s",(utterance['speaker_id'],utterance['order'],utterance['corpus_id'],transcript['id'],))
                                
                                ut = utest_cursor.fetchall()

                                # utest_cursor.close()

                                length = 0

                                if len(ut)>0:
                                    utterance_id = ut[0][0]
                                    if ut[0][1] == None:
                                        length = 0
                                    else: 
                                        length = ut[0][1]
                                else:
                                    sql = "insert into utterance_jap (speaker_id,`order`, corpus_id, speaker_age, speaker_code, speaker_name, speaker_role, speaker_sex, target_child_id, target_child_name, target_child_sex, target_child_age, transcript_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                    # print(sql)
                                    insert_cursor.execute(sql, (utterance['speaker_id'],utterance['order'],utterance['corpus_id'],speaker[12],speaker[1],   speaker[2],     speaker[3],  speaker[6],  speaker[13],     target_child_name, target_child_age, target_child_age, transcript['id'] ))
                                    # "insert into utterance_jap (speaker_id,             `order`,            corpus_id,            speaker_age, speaker_code, speaker_name, speaker_role, speaker_sex, target_child_id, target_child_name, target_child_sex, target_child_age) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                                    db.commit()

                                    utterance_id = insert_cursor.lastrowid

                                    # insert_cursor.close()

                                i = 0
                                for token in child:
                                    if get_node_name(token.tag) in ['w','t','tagMarker','g']:
                                        tk = {
                                            'speaker_id':speaker[0],
                                            'utterance_id': utterance_id,
                                            'token_order':i,
                                            'corpus_id':corpus['id']
                                        }
                                        tk_test_cursor = db.cursor(prepared=True)
                                        tk_test_cursor.execute("SELECT * FROM token_jap WHERE utterance_id = %s AND token_order = %s",(tk['utterance_id'],tk['token_order'],))
                                        res = tk_test_cursor.fetchall()
                                        # tk_test_cursor.close()
                                        if len(res) == 0: # Se o token não foi cadastrado ainda
                                            if get_node_name(token.tag) in ['w','g']:
                                                if get_node_name(token.tag) == 'g':
                                                    # print(list(token.iter()))
                                                    # for ssds in token:
                                                    #     print(ssds)
                                                    #     print('\n')
                                                    a = token.find(ns+'w')
                                                    if a == None:
                                                        a = token.find(ns+"g").find(ns+'w')
                                                    token = a
                                                    
                                                # WORD
                                                # print(token.text)
                                                # print(token)
                                                tk['gloss'] = token.text
                                                replacement = token.find(ns+'replacement')
                                                if replacement != None:
                                                    tk['replacement'] = replacement.text
                                                    token = replacement

                                                if token.find(ns+'mor') != None:
                                                    if token.find(ns+'mor').find(ns+'mwc') != None:
                                                        tk['part_of_speech'] = token.find(ns+'mor').find(ns+'mwc').find(ns+'pos').find(ns+'c').text
                                                        stems = []
                                                        for mw in token.find(ns+'mor').find(ns+'mwc').findall(ns+'mw'):
                                                            stems.append(mw.find(ns+"stem").text)
                                                        tk['stem'] = ''.join(stems)
                                                    else:
                                                        tk['part_of_speech'] = token.find(ns+'mor').find(ns+'mw').find(ns+'pos').find(ns+'c').text
                                                        tk['stem'] = token.find(ns+'mor').find(ns+'mw').find(ns+'stem').text

                                                    gra = token.find(ns+'mor').find(ns+'gra')
                                                    if gra != None:
                                                        tk['relation'] = gra.attrib['index']+"|"+gra.attrib['head']+"|"+gra.attrib['relation']
                                                
                                                if token.find(ns+"shortening") != None:
                                                    if 'stem' not in tk:
                                                        tk['gloss'] = token.text
                                                        tk['stem'] = token.text
                                                    else:
                                                        tk['gloss']  = tk['stem']

                                                
                                                if tk['gloss'] == None:
                                                    tk['gloss'] = tk['stem']
                                                
                                                
                                            elif get_node_name(token.tag) == 't':
                                                #punctuation
                                                _type = token.attrib['type']
                                                if _type in punctuation_mapping:
                                                    tk['gloss'] = punctuation_mapping[_type]
                                                    tk['stem'] = punctuation_mapping[_type]
                                                    tk['part_of_speech'] = "f"
                                                    
                                                    if token.find(ns+'mor') != None:
                                                        gra = token.find(ns+'mor').find(ns+'gra')
                                                        if gra != None:
                                                            tk['relation'] = gra.attrib['index']+"|"+gra.attrib['head']+"|"+gra.attrib['relation']

                                            elif get_node_name(token.tag) in ['tagMarker','pause']:
                                                tk['gloss'] = ','
                                                tk['stem'] = ','
                                                tk['part_of_speech'] = "f"
                                                
                                                if token.find(ns+'mor') != None:
                                                    gra = token.find(ns+'mor').find(ns+'gra')
                                                    if gra != None:
                                                        tk['relation'] = gra.attrib['index']+"|"+gra.attrib['head']+"|"+gra.attrib['relation']
                                                # comma, etc
                                                pass
                                            else:
                                                print(">>>>>>>>>>>> "+get_node_name(token.tag))
                                            i = i +1
                                            print("\t\t"+tk['gloss'])
                                            
                                            insert('token_jap',tk)
                                            # j = j + 1
                                            # if j == 100:
                                            #     db.close()
                                            #     db = connect()
                                            #     j = 0
                                        del tk
                                update_cursor = db.cursor()
                                if i != 0:
                                    print(length, i)
                                    length = length + (i + 1)
                                    update_cursor.execute("UPDATE utterance_jap SET length = %s WHERE id = %s",(length, utterance_id,))
                                    db.commit()
                                    # update_cursor.close()
                                # print('\n')

                    a = db.cursor()

                    a.execute("UPDATE transcript_jap SET finished = 1 WHERE id = %s",(transcript['id'],))
                        


                                