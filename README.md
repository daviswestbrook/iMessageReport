# iMessageReport
iMessage Report analyzes iMessage data stored in MacBooks to give insight into relationships and habits

Tapback data (iMessage reactions such as "like", "emphasize", etc) is the primary focus of this project. Since tapback data is not persisted, the messages are manipulated to accurately impute this valuable information. To my research, this function has nowhere else been (publicly) implemented.

To simply pull a message stream's data to a CSV, use pull-group.py.

To then run impute tapback data, use run-sample.py.

To do both, use main.py.

Additional functionality (including an advnaced report) coming soon.




