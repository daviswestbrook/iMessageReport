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

# Build group
print("building group")
group1 = Group.Group(group_ID, messages)
group1.write_to_csv()
group2 = Group.Group(group_ID)

print(group1)
print(group2)

# Make report

# Close log
log.close()