"""
Bot commands
"""

from telegram import Update
from telegram.ext import ContextTypes
import datetime
import logging

from _my_modules import funcconf
from _my_modules import funcdatn
from _my_modules import funcdatn
from _my_modules import funcsms


l_debug: bool = False

# ENABLE LOGGING
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# INDEX
"""
MESSAGE WHEN THE /hi command is issued
SEND A MESSAGE WHEN THE command /help is issued
ECHO THE USER MESSAGE
FIND A NAME, ID NWU NUMBER WHEN THE /name command is issued
RUN A SCRIPT WHEN THE /run command is issued
SET A SCHEDULE WHEN THE /set command is issued
SET A SWITCH WHEN THE /switch command is issued
STOP THE ROBOT
LOG ERRORS
"""

# TELEGRAM COMMAND LIST
"""
hi - Hello
help - List of commands
name - Search names and numbers
"""


async def hi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text=update.message.text)


async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Sorry, I didn't understand that command.")
