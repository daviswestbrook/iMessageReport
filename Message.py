import MacOSLibraryTools as db
import pandas as pd

class Message:
    def __init__(self, text, sender_ID):
        self.text = text
        self.sender_ID = sender_ID
        self.timestamp = timestamp



def get_message_stream():
    messages = db.get_all_messages()
    messages.rename(columns={'ROWID' : 'message_id'}, inplace = True)
    return messages

def get_message_stream_by_group(messages, group_ID, texters): # needs to handle people who are no longer in the group!
    merged = pd.merge(messages[['text', 'handle_id', 'date', 'message_id', 'cache_has_attachments', 'date_uct']],  texters[['handle_id', 'phone_number']], on ='handle_id', how='left')
    chat_message_joins = db.read_all_from_table('chat_message_join')
    messages = pd.merge(merged, chat_message_joins[['chat_id', 'message_id']], on = 'message_id', how='left')
    messages = messages.loc[messages['chat_id'] == group_ID]
    messages = messages.reset_index(drop=True)
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
    return pd.read_csv(str(group_ID) + ".csv")






        
    