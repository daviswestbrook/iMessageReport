import message
import group



# Clear log
log = open("log.txt", "a+")
log.truncate(0)


# Build group from sample csv
print("building group")
group_ID = group.specify_group()
group1 = group.Group(group_ID)

message.generate_reactors(group1.messages)

group1.messages.to_csv("test.csv")

# Make report


# Close log
log.close()