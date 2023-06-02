import message
import group



# Clear log
log = open("log.txt", "a+")
log.truncate(0)

# Get messages (DataFrame)
print("getting all messages...")
messages = message.get_message_stream()

# Find groups
print("getting groups")
groups = group.get_groups()

# Ask user to specify group
print("specifying groups")
group_ID = group.specify_group()

# Build group
print("building group")
group1 = group.Group(group_ID, messages)
group1.write_to_csv()

# Make report

# Close log
log.close()