import Message
import Group



# Clear log
log = open("log.txt", "a+")
log.truncate(0)

# Get messages (DataFrame)
print("getting all messages...")
messages = Message.get_message_stream()

# Find groups
print("getting groups")
groups = Group.get_groups()

# Ask user to specify group
print("specifying groups")
group_ID = Group.specify_group()

# Get handles in the group and clean up texters with user help
print("making texters")
texters = Group.make_texters(group_ID)

print("narrowing messages to group")
group_messages = Message.get_message_stream_by_group(messages, group_ID, texters)

print("done")
print(group_messages)

# Make report



# Close log
log.close()