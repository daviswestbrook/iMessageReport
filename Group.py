import MacOSLibraryTools as db

GROUP_NAME = "Let’s body this bus"

def Group:
    __init__(self, group_ID):
        self.group_ID = group_ID

def get_groups():
    chat_records = db.read_all_from_table(chat)
    not_null_msk = chat_records['display_name'].notnull()
    cols = ['ROWID', 'display_name']
    group_names = chat_records.loc[not_null_msk, cols]
    return group_names
    

def specify_group():
    #TODO
    
    chat_records = db.read_all_from_table(chat)
    CHAT_ID = chat_records.loc[chat['display_name'] == GROUP_NAME]['ROWID'].values[0]
    return CHAT_ID


    

def make_texters(group_ID):
    handles = get_handles(group_ID)
    texters = consolidate_handles(handles)
    return texters


def get_handles(group_ID):
    handles = db.read_all_from_table(handle)
    handles.rename(columns={'id' : 'phone_number', 'ROWID': 'handle_id'}, inplace = True)
    return handles

def consolidate_handles(handles):
    #TODO

    return handles

