import dbtools as db
import pandas as pd

class Message:
    def __init__(self, text, sender_ID):
        self.text = text
        self.sender_ID = sender_ID
        self.timestamp = timestamp

def get_message_stream():
    print("get_message_stream")
    messages = db.get_all_messages()
    messages.rename(columns={'ROWID' : 'message_id'}, inplace = True)
    return messages

def get_message_stream_by_group(messages, group_ID, texters): 
    print("get_message_stream_by_group")
    merged = pd.merge(messages[['text', 'handle_id', 'date', 'message_id', 'cache_has_attachments', 'date_uct']],  texters[['handle_id', 'phone_number']], on ='handle_id', how='left')
    chat_message_joins = db.read_all_from_table('chat_message_join')
    messages = pd.merge(merged, chat_message_joins[['chat_id', 'message_id']], on = 'message_id', how='left')
    messages = messages.loc[messages['chat_id'] == group_ID]
    messages = messages.reset_index(drop=True)
    messages.sort_values(by=['date'], inplace=True)
    messages.reset_index(drop=True, inplace=True)
    print("Handling group members...")
    #chat_handle = db.read_all_from_table('chat_handle_join', 'chat_id = ' + str(group_ID))
    return messages

def add_object_column_to_stream(stream, name):
    stream[name] = pd.Series(dtype='object')
    
def add_reaction_columns(stream):
    add_object_column_to_stream(stream, 'liked')
    add_object_column_to_stream(stream, 'loved')
    add_object_column_to_stream(stream, 'laughed')
    add_object_column_to_stream(stream, 'disliked')
    add_object_column_to_stream(stream, 'emphasized')

def get_message_stream_from_csv(group_ID):
    print("get_message_stream_from_csv")
    stream = pd.read_csv(str(group_ID) + ".csv",  dtype={"liked":"object", "loved":"object", "laughed":"object", "disliked":"object", "emphasized":"object", "text":"str", "chat_id": "int64"}, keep_default_na=False)
    stream["text"] = stream["text"].values.astype(str)
    stream.sort_values(by=['date'], inplace=True)
    stream.reset_index(drop=True, inplace=True)
    #print(stream.dtypes)
    return stream


def generate_reactors(stream): # assumes names have already been filled in
    # to handle weird formatting for different message types
    log = open("log_generate_reactors.txt", "a+")
    log.truncate(0)
    attachments = ['n imag', 'n attachmen', ' contac', ' movi', 'n audio messag']
    for i, msg in stream.iterrows():
        print(msg)
        if type(msg['text']) == str:
            found = True
        
            ### likes ###
            if msg['text'][:5] == 'Liked':
                length = len(msg['text'])
                cutoff = min(27, length-1)
                quoted = msg['text'][7:cutoff] 
                target_length = len(quoted)
                a = 1
                #log.write(str(i) + "\n" + quoted + "\n")
            
                if quoted in attachments:
                    while (stream['cache_has_attachments'][i - a] == 0):
                        a += 1
                    
                        if a > 100:
                            found = False
                            break          
                else:
                    while (stream['text'][i - a].replace("\ufffc", "")[:min(20,len(stream['text'][i-a]))] != quoted):
                        #if i==22969:
                         #   log.write(stream['text'][i - a].replace("\ufffc", "")[:min(20,len(stream['text'][i-a]))] + "\n")
                         #   log.write(quoted)
                        a += 1;
                        if a > 100: 
                            found = False
                            break
                    
                if found:
                    #liker = indices[msg['handle_id']]
                    #likee = indices[messages['handle_id'][i - a]]
                    if not stream.at[i-a, 'liked']:
                        stream.at[i-a, 'liked'] = [msg['handle_id']]
                    else:
                        stream.at[i-a, 'liked'].append(msg['handle_id'])
                        
                    #like_series.iat[liker] +=1
                    #like_frame.iat[liker, likee] += 1
                else: 
                    message = "like not found at " + str(i)
                    log.write(message + "\n")

            ### loves ###
            if msg['text'][:5] == 'Loved':
                length = len(msg['text'])
                cutoff = min(27, length-1)
                quoted = msg['text'][7:cutoff] 
                target_length = len(quoted)
                a = 1
            
                if quoted in attachments:
                    while (stream['cache_has_attachments'][i - a] == 0):
                        a += 1
                    
                        if a > 100:
                            found = False
                            break          
                else:
                    while (stream['text'][i - a].replace("\ufffc", "")[:min(20,len(stream['text'][i-a]))] != quoted):
                        a += 1;
                        if a > 100: 
                            found = False
                            break
                    
                if found:
                    if not stream.at[i-a, 'loved']:
                        stream.at[i-a, 'loved'] = [msg['handle_id']]
                    else:
                        stream.at[i-a, 'loved'].append(msg['handle_id'])
                else: 
                    message = "love not found at " + str(i)
                    log.write(message + "\n")

            ### laughs ###
            if msg['text'][:10] == 'Laughed at':
                length = len(msg['text'])
                cutoff = min(32, length-1)
                quoted = msg['text'][12:cutoff] 
                target_length = len(quoted)
                a = 1
            
                if quoted in attachments:
                    while (stream['cache_has_attachments'][i - a] == 0):
                        a += 1
                    
                        if a > 100:
                            found = False
                            break          
                else:
                    while (stream['text'][i - a].replace("\ufffc", "")[:min(20,len(stream['text'][i-a]))] != quoted):
                        a += 1;
                        if a > 100: 
                            found = False
                            break
                    
                if found:
                    if not stream.at[i-a, 'laughed']:
                        stream.at[i-a, 'laughed'] = [msg['handle_id']]
                    else:
                        stream.at[i-a, 'laughed'].append(msg['handle_id'])
                else: 
                    message = "laugh not found at " + str(i)
                    log.write(message + "\n")

            ### dislikes ###
            if msg['text'][:8] == 'Disliked':
                length = len(msg['text'])
                cutoff = min(30, length-1)
                quoted = msg['text'][10:cutoff] 
                target_length = len(quoted)
                a = 1
            
                if quoted in attachments:
                    while (stream['cache_has_attachments'][i - a] == 0):
                        a += 1
                    
                        if a > 100:
                            found = False
                            break          
                else:
                    while (stream['text'][i - a].replace("\ufffc", "")[:min(20,len(stream['text'][i-a]))] != quoted):
                        a += 1;
                        if a > 100: 
                            found = False
                            break
                    
                if found:
                    if not stream.at[i-a, 'disliked']:
                        stream.at[i-a, 'disliked'] = [msg['handle_id']]
                    else:
                        stream.at[i-a, 'disliked'].append(msg['handle_id'])
                else: 
                    message = "dislike not found at " + str(i)
                    log.write(message + "\n")

            ### emphasizes ###
            if msg['text'][:10] == 'Emphasized':
                length = len(msg['text'])
                cutoff = min(32, length-1)
                quoted = msg['text'][12:cutoff] 
                target_length = len(quoted)
                a = 1
            
                if quoted in attachments:
                    while (stream['cache_has_attachments'][i - a] == 0):
                        a += 1
                    
                        if a > 100:
                            found = False
                            break          
                else:
                    while (stream['text'][i - a].replace("\ufffc", "")[:min(20,len(stream['text'][i-a]))] != quoted):
                        a += 1;
                        if a > 100: 
                            found = False
                            break
                    
                if found:
                    if not stream.at[i-a, 'emphasized']:
                        stream.at[i-a, 'emphasized'] = [msg['handle_id']]
                    else:
                        stream.at[i-a, 'emphasized'].append(msg['handle_id'])
                else: 
                    message = "emphasized not found at " + str(i)
                    log.write(message + "\n")


    log.close()




        
    