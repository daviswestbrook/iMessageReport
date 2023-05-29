#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 24 22:27:22 2021

@author: daviswestbrook
"""

import sqlite3
import pandas as pd
import numpy as np
import difflib

GROUP_NAME = "Let’s body this bus"
USERNAME = 'daviswestbrook'

# establish connection
print("Connecting to database...")
conn = sqlite3.connect('/Users/' + USERNAME + '/Library/Messages/chat.db')

# connect to the database
cur = conn.cursor()

log = open("log.txt", "a+")
log.truncate(0)
#%%

# # get the names of the tables in the database
'''
cur.execute('select name from sqlite_master where type = "table"')
for name in cur.fetchall():
    print(name)
'''
#%%

# pd dataframe of all messages
messages = pd.read_sql_query('select *, datetime(message.date/1000000000 + strftime("%s", "2001-01-01") ,"unixepoch","localtime") as date_uct from message', conn)

#%%

# get the handles to apple-id mapping table
print("finding handle id mapping...")
handles = pd.read_sql_query('select * from handle', conn)

# and join to the messages, on handle_id
messages.rename(columns={'ROWID' : 'message_id'}, inplace = True)
handles.rename(columns={'id' : 'phone_number', 'ROWID': 'handle_id'}, inplace = True)
merged = pd.merge(messages[['text', 'handle_id', 'date', 'message_id', 'cache_has_attachments', 'date_uct']],  handles[['handle_id', 'phone_number']], on ='handle_id', how='left')

#%%

# get the chat to message mapping
print("Finding chat mapping...")
chat_message_joins = pd.read_sql_query('select * from chat_message_join', conn)

# and merge to add chat_id

messages = pd.merge(merged, chat_message_joins[['chat_id', 'message_id']], on = 'message_id', how='left')

# find specific chat_id for relevant group 
chat = pd.read_sql_query('select * from chat', conn)
'''
print(type(chat))
print(chat['display_name'].head(20))
print(chat.columns)
'''
CHAT_ID = chat.loc[chat['display_name'] == GROUP_NAME]['ROWID'].values[0]


#print(CHAT_ID)

# slim down dataframe to only those messages in relevant chat
print("Slimming down message DataFrame")
messages = messages.loc[messages['chat_id'] == CHAT_ID]
messages = messages.reset_index(drop=True)
#%%

# find all the group members of relevant group
print("Handling group members...")
chat_handle = pd.read_sql_query('select * from chat_handle_join where chat_id = ' + str(CHAT_ID), conn)

# create and fill dictionary relating each member to their index (sorted by handle_id)
member_count = 1
indices = {0:0}
for handle in chat_handle["handle_id"].values:
    indices[handle] = member_count
    member_count += 1

print(indices)

#%%
print("Initializing tables...")
activity_list = [0]*len(indices);
activity = pd.Series(activity_list)
like_counter = 0
# e.g. row will represent liker, column will represent likee
like_frame = pd.DataFrame(0, index=np.arange(member_count), columns=np.arange(member_count))
love_frame = pd.DataFrame(0, index=np.arange(member_count), columns=np.arange(member_count))
laugh_frame = pd.DataFrame(0, index=np.arange(member_count), columns=np.arange(member_count))
dislike_frame = pd.DataFrame(0, index=np.arange(member_count), columns=np.arange(member_count))

# each person total count for each reaction
like_series = pd.Series(activity_list)
love_series = pd.Series(activity_list)
laugh_series = pd.Series(activity_list)
dislike_series = pd.Series(activity_list)
early_bird_series = pd.Series(activity_list)
ln_spec_series = pd.Series(activity_list)

#%%
# to handle weird formatting for different message types
miscellaneous = ['n imag', 'n attachmen', ' contac', ' movi', 'n audio messag']

######## LOOP ########
print("Filling tables...")
for i, msg in messages.iterrows():
    
    if 0 <= int(msg['date_uct'][11:13]) <= 4:
        ln_spec_series.iat[indices[msg['handle_id']]] += 1
    elif 5 <= int(msg['date_uct'][11:13]) <= 8:
        early_bird_series.iat[indices[msg['handle_id']]] += 1
    
    if type(msg['text']) == str:
        found = True
        
        if msg['handle_id'] not in indices:
            message = 'unknown member detected'
            log.write(message + "\n")
            indices[msg["handle_id"]] = member_count
            activity = activity.append(pd.Series([0]), ignore_index=True)
            like_frame[member_count] = 0
            like_frame = like_frame.append(pd.Series(0, index=like_frame.columns), ignore_index=True)
            like_series = like_series.append(pd.Series([0]), ignore_index=True)
            love_frame[member_count] = 0
            love_frame = love_frame.append(pd.Series(0, index=love_frame.columns), ignore_index=True)
            love_series = love_series.append(pd.Series([0]), ignore_index=True)
            laugh_frame[member_count] = 0
            laugh_frame = laugh_frame.append(pd.Series(0, index=laugh_frame.columns), ignore_index=True)
            laugh_series = laugh_series.append(pd.Series([0]), ignore_index=True)
            dislike_frame[member_count] = 0
            dislike_frame = dislike_frame.append(pd.Series(0, index=dislike_frame.columns), ignore_index=True)
            dislike_series = dislike_series.append(pd.Series([0]), ignore_index=True)
            early_bird_series = early_bird_series.append(pd.Series([0]), ignore_index=True)
            ln_spec_series = ln_spec_series.append(pd.Series([0]), ignore_index=True)
            member_count += 1
        activity.iat[indices[msg['handle_id']]] += 1

        ### likes ###
        if msg['text'][:5] == 'Liked':
            quoted = msg['text'][7:-1]     # original message
            a = 1
            
            if quoted in miscellaneous:
                while (messages['cache_has_attachments'][i - a] == 0):
                    a += 1
                    
                    if a > 100:
                        found = False
                        break          
            else:
                while (messages['text'][i - a].replace("\ufffc", "") != quoted):
                    a += 1;
                    if a > 100: 
                        found = False
                        break
                    
            if found:
                liker = indices[msg['handle_id']]
                likee = indices[messages['handle_id'][i - a]]
                like_series.iat[liker] +=1
                like_frame.iat[liker, likee] += 1
            else: 
                message = "like not found at " + str(i)
                log.write(message + "\n")

        
        ### loves ###
        elif msg['text'][:5] == 'Loved':
            quoted = msg['text'][7:-1]     # original message
            a = 1
            
            if quoted in miscellaneous:
                while (messages['cache_has_attachments'][i - a] == 0):
                    a += 1
                    
                    if a > 100:
                        found = False
                        break
            else:
                while (messages['text'][i - a].replace("\ufffc", "") != quoted):
                    a += 1;
                    
                    if a > 100:
                        found = False
                        break
                    
            if found:
                lover = indices[msg['handle_id']]
                lovee = indices[messages['handle_id'][i - a]]
                love_series.iat[lover] += 1
                love_frame.iat[lover, lovee] += 1
            else:
                message = "love not found at " + str(i)
                log.write(message + "\n")

        
        # Laughs
        elif msg['text'][:10] == 'Laughed at':
            quoted = msg['text'][12:-1]     # original message
            a = 1
            
            if quoted in miscellaneous:
                while (messages['cache_has_attachments'][i - a] == 0):
                    a += 1
                    
                    if a > 100:
                        found = False
                        break
            else:
                while (messages['text'][i - a].replace("\ufffc", "") != quoted):
                    # if i==75 and a==1:
                    #     print(ascii(messages['text'][i - a]), ascii(quoted))
                    a += 1
                    
                    if a > 100 or i - a == 0:
                        found = False
                        break
                    
            if found:
                laugher = indices[msg['handle_id']]
                laughee = indices[messages['handle_id'][i - a]]
                laugh_series.iat[laugher] += 1
                laugh_frame.iat[laugher, laughee] += 1
            else:
                message = "laugh not found at " + str(i)
                log.write(message + "\n")
            
        # Dislike
        elif msg['text'][:8] == 'Disliked':
            quoted = msg['text'][10:-1]     # original message
            a = 1
            
            if quoted in miscellaneous:
                while (messages['cache_has_attachments'][i - a] == 0):
                    a += 1
                    
                    if a > 100:
                        found = False
                        break
            else:
                while (messages['text'][i - a].replace("\ufffc", "") != quoted):
                    a += 1;
                    
                    if a > 100:
                        found = False
                        break
                    
            if found:
                disliker = indices[msg['handle_id']]
                dislikee = indices[messages['handle_id'][i - a]]
                dislike_series.iat[disliker] += 1
                dislike_frame.iat[disliker, dislikee] += 1
            else:
                message = "Dislike not found at " + str(i)
                log.write(message + "\n")

    else:  #if not a string
        messages.loc[i, "text"] = ""
        
print("Successfully parsed through", i, "messages.")
#%%
print("Building report...")

like_coef = like_frame.div(activity, axis=1).div(like_series**.5 + 10, axis=0)
love_coef = love_frame.div(activity, axis=1).div(love_series**.5 + 10, axis=0)
laugh_coef = laugh_frame.div(activity, axis=1).div(laugh_series**.5 + 10, axis=0)
dislike_coef = dislike_frame.div(activity, axis=1).div(dislike_series**.5 + 10, axis=0)

early_bird_coef = early_bird_series.div(activity)
ln_spec_coef = ln_spec_series.div(activity)

combined = like_coef.add(love_coef.mul(2, fill_value=0).add(laugh_coef, fill_value=0), fill_value=0).sub(dislike_coef, fill_value=0) * 10000

friends_enemies = pd.DataFrame(0.0, index=np.arange(member_count), columns=np.arange(member_count))
lopsided = pd.DataFrame(0.0, index=np.arange(member_count), columns=np.arange(member_count))

j = 0
while (j < member_count):
    k = j + 1
    while (k < member_count):
        friends_enemies.iat[j, k] += combined.iat[j, k] + combined.iat[k, j]
        lopsided.iat[j, k] += abs(combined.iat[j, k] - combined.iat[k, j])
        k += 1
    j += 1



# (value, liker, likee)
friends_final = []
enemies_final = []
lop_final = []
early_bird_final = []
ln_spec_final = []

for i in range(member_count):
    early_bird_final.append([early_bird_coef.iat[i], i])
    ln_spec_final.append([ln_spec_coef.iat[i], i])
    if len(early_bird_final) > 3:
        early_bird_final = sorted(early_bird_final, key = lambda x: x[0], reverse=True)
        early_bird_final = early_bird_final[:3]
        ln_spec_final = sorted(ln_spec_final, key = lambda x: x[0], reverse=True)
        ln_spec_final = ln_spec_final[:3]
        
j = 0
while (j < member_count):
    k = j + 1
    while (k < member_count):
        friends_final.append([friends_enemies.iat[j, k], j, k])
        enemies_final.append([friends_enemies.iat[j, k], j, k])
        lop_final.append([lopsided.iat[j, k], j, k])
        if len(friends_final) > 3:
            friends_final = sorted(friends_final, key = lambda x: x[0], reverse=True)
            enemies_final = sorted(enemies_final, key = lambda x: x[0])
            lop_final = sorted(lop_final, key = lambda x: x[0], reverse=True)
            friends_final = friends_final[:3]
            enemies_final = enemies_final[:3]
            lop_final = lop_final[:3]
            
        k += 1
    j += 1

#%%

self_info = pd.DataFrame({"chat_id" : 200, "handle_id": 0}, index = [0])
chat_handle = pd.concat([self_info, chat_handle]).reset_index(drop=True)
self_info2 = pd.DataFrame({"handle_id": 0, "phone_number": "+6156131887"}, index = [0])
handles = pd.concat([self_info2, handles]).reset_index(drop=True)

for r in friends_final:
    a = chat_handle.at[r[1],"handle_id"]
    a1 = chat_handle.at[r[2],"handle_id"]
    b = handles.index[handles.handle_id == a][0]
    b1 = handles.index[handles.handle_id == a1][0]
    r.append(r[1])
    r.append(r[2])
    r[1] = handles.at[b, "phone_number"]
    r[2] = handles.at[b1, "phone_number"]

for r in enemies_final:
    a = chat_handle.at[r[1],"handle_id"]
    a1 = chat_handle.at[r[2],"handle_id"]
    b = handles.index[handles.handle_id == a][0]
    b1 = handles.index[handles.handle_id == a1][0]
    r.append(r[1])
    r.append(r[2])
    r[1] = handles.at[b, "phone_number"]
    r[2] = handles.at[b1, "phone_number"]

for r in lop_final:
    a = chat_handle.at[r[1],"handle_id"]
    a1 = chat_handle.at[r[2],"handle_id"]
    b = handles.index[handles.handle_id == a][0]
    b1 = handles.index[handles.handle_id == a1][0]
    r.append(r[1])
    r.append(r[2])
    r[1] = handles.at[b, "phone_number"]
    r[2] = handles.at[b1, "phone_number"]
 
for r in early_bird_final:
    a = chat_handle.at[r[1],"handle_id"]
    b = handles.index[handles.handle_id == a][0]
    r.append(r[1])
    r[1] = handles.at[b, "phone_number"]
    
for r in ln_spec_final:
    a = chat_handle.at[r[1],"handle_id"]
    b = handles.index[handles.handle_id == a][0]
    r.append(r[1])
    r[1] = handles.at[b, "phone_number"]


print("Report Complete...")

print("Early Birds:")

print("1. " + early_bird_final[0][1])
print("2. " + early_bird_final[1][1]) 
print("3. " + early_bird_final[2][1])

print("-------------------------------")

print("Late Night Specialists:")

print("1. " + ln_spec_final[0][1])
print("2. " + ln_spec_final[1][1]) 
print("3. " + ln_spec_final[2][1]) 

print("-------------------------------")

print("Biggest Friends:")

print("1. " + friends_final[0][1] + "  and  " + friends_final[0][2])
print("2. " + friends_final[1][1] + "  and  " + friends_final[1][2])
print("3. " + friends_final[2][1] + "  and  " + friends_final[2][2])

print("-------------------------------")

print("Most Lopsided Relationships:")   

print("1. " + lop_final[0][1] + "  and  " + lop_final[0][2])
print("2. " + lop_final[1][1] + "  and  " + lop_final[1][2])
print("3. " + lop_final[2][1] + "  and  " + lop_final[2][2])

print("-------------------------------")

print("Biggest Enemies:")

print("1. " + enemies_final[0][1] + "  and  " + enemies_final[0][2])
print("2. " + enemies_final[1][1] + "  and  " + enemies_final[1][2])
print("3. " + enemies_final[2][1] + "  and  " + enemies_final[2][2])
    
print("-------------------------------")
    
log.close()



#%%

# conn2 = sqlite3.connect('/Users/' + USERNAME + '/Library/Application Support/AddressBook/ABAssistantChangelog.aclcddb')

# cur2 = conn2.cursor()

# cur2.execute('select name from sqlite_master where type = "table"')
# for name in cur2.fetchall():
#     print(name)


print(indices)




