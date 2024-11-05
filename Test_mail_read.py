from exchangelib import Credentials, Account, DELEGATE, Message

# Replace with your credentials
email = 'albert.jansevanrensburg@nwu.ac.za'
password = 'FAgd5meM^ua#'

# Set up the credentials
credentials = Credentials(
    username=email,
    password=password
)

# Connect to the account
account = Account(
    primary_smtp_address=email,
    credentials=credentials,
    autodiscover=True,
    access_type=DELEGATE
)

# Access the inbox
inbox = account.inbox

# Print subject, sender, and body of each email
for item in inbox.all().order_by('-datetime_received')[:10]:  # Fetch last 10 emails
    if isinstance(item, Message):
        print(f"Subject: {item.subject}")
        print(f"From: {item.sender.name} <{item.sender.email_address}>")
        print(f"Body: {item.text_body}\n")
        print("-" * 60)