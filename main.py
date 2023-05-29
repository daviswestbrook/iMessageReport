import Message
import Group
import Report


# Clear log
log = open("log.txt", "a+")
log.truncate(0)

# Get messages (DataFrame)
messages = Message.get_message_stream()

# Find groups
groups = Group.get_groups()

# Ask user to specify group
group_ID = Group.specify_groups()

# Get handles in the group and clean up texters with user help
texters = Group.make_texters(group_ID)


group_messages = Message.get_message_stream_by_group(messages, group_ID, texters)


# Make report



# Close log
log.close()