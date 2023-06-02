import dbtools as db
import message
GROUP_NAME = "Let’s body this bus"


class Group:
    def __init__(self, group_ID, messages=None):
        print("group __init__")
        self.group_ID = group_ID
        texter_mapping = make_texters(group_ID)
        if messages is not None:
            self.messages =  message.get_message_stream_by_group(messages, group_ID, texter_mapping)
            message.add_reaction_columns(self.messages)

            self.member_mapping = make_members(self.messages, texter_mapping)
            self.members = set(self.member_mapping.values())
            self.messages["name"] = self.messages["handle_id"].map(self.member_mapping)
        else:
            self.messages = message.get_message_stream_from_csv(group_ID)
            self.members = set(self.messages['name'].unique())
        
 
    def __str__(self):
        output = "Group ID: {}".format(self.group_ID)
        output += "\nMessages: {}".format(self.messages)
        output += "\nMembers: {}".format(str(self.members))

        return output



    def write_to_csv(self):
        filename = str(self.group_ID) + ".csv"
        self.messages.to_csv(filename)
        
def get_groups():
    print("get_groups")
    chat_records = db.read_all_from_table('chat')
    not_null_msk = chat_records['display_name'].notnull()
    cols = ['ROWID', 'display_name']
    group_names = chat_records.loc[not_null_msk, cols]
    return group_names
    
def specify_group():
    #TODO
    chat_records = db.read_all_from_table('chat')
    CHAT_ID = chat_records.loc[chat_records['display_name'] == GROUP_NAME]['ROWID'].values[0]
    return CHAT_ID

def make_texters(group_ID):
    handles = get_handles(group_ID)
    texters = consolidate_handles(handles)
    return texters

def get_handles(group_ID):
    #currently this is for all messages (not just the group)
    handles = db.read_all_from_table('handle')
    handles.rename(columns={'id' : 'phone_number', 'ROWID': 'handle_id'}, inplace = True)
    return handles

def consolidate_handles(handles):
    #TODO

    return handles

def make_members(stream, mapping):
    print("make_members")
    handle_to_member = {}
    unique = stream['handle_id'].unique()
    for handle in unique:
        if handle==0:
            print("what is your name?")
        else:
            number = mapping.loc[mapping['handle_id'] == handle, 'phone_number'].iloc[0]
            print("who is {}?".format(number))
        name = input()
        handle_to_member[handle] = name
    return handle_to_member
    
