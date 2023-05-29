import MacOSLibraryTools as db

def Message:
    __init__(self, text, sender_ID):
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
    chat_handle = pd.read_sql_query('select * from chat_handle_join where chat_id = ' + str(CHAT_ID), conn)
    return messages







        
    