from aiogram.fsm.state import State, StatesGroup


class AdminStates(StatesGroup):
    waiting_user_id = State()
    waiting_admin_id = State()
    waiting_broadcast = State()
