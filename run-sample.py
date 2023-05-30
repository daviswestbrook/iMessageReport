import Message
import Group



# Clear log
log = open("log.txt", "a+")
log.truncate(0)


# Build group from sample csv
print("building group")
group_ID = Group.specify_group()
group1 = Group.Group(group_ID)

Message.populate_reactors(group1.messages)

group1.messages.to_csv("test.csv")

# Make report


# Close log
log.close()