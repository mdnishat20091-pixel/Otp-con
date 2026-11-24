from aiogram.fsm.state import State, StatesGroup


class RecoveryStates(StatesGroup):
    in_progress = State()
