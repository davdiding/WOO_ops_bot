from lib.utils import Tools
from telegram import Chat, ChatMember, ChatMemberUpdated, Update
from telegram.ext import (
    Application,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


class InfoBot:
    BOT_KEY = "INFO_BOT_KEY"
    
    def __init__(self):
        self.tools = Tools()
        self.logger = self.tools.get_logger("info")
        
    def run(self):
        application = Application.builder().token(self.tools.config[self.BOT_KEY]).build()
        application.run_polling()


if __name__ == '__main__':
    info_bot = InfoBot()
    info_bot.run()
    
    