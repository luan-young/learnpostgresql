import random
from typing import List
from connections import create_connection
from models.poll import Poll
from models.option import Option
import database

MENU_PROMPT = """--- Menu ---

1) Create new poll
2) List opel polls
3) Vote on a poll
4) Show poll votes
5) Select a random winner from a poll option
6) Exit

Enter your choice: """

NEW_OPTION_PROMPT = "Enter new option text (or leave empty to stop adding options: "


def prompt_create_poll():
    poll_title = input("Enter poll title: ")
    poll_owner = input("Enter poll owner: ")
    poll = Poll(poll_title, poll_owner)
    poll.save()
    while new_option := input(NEW_OPTION_PROMPT):
        poll.add_option(new_option)


def list_open_polls():
    for poll in Poll.all():
        print(poll)


def prompt_vote_poll():
    poll_id = int(input("Enter poll would you like to vote on: "))
    poll = Poll.get(poll_id)
    if not poll:
        print("Invalid poll.")
        return

    options = poll.options
    if not len(options):
        print("No options available for this poll.")
        return

    _print_poll_options(options)

    option_id = int(input("Enter option you would like to vote for: "))
    username = input("Enter the username you would like to vote as: ")
    option = Option.get(option_id)
    if not option:
        print("Invalid option.")
        return
    option.vote(username)


def _print_poll_options(options: List[Option]):
    for option in options:
        print(option)


def show_poll_votes():
    poll_id = int(input("Enter poll you would like to see votes for: "))
    poll = Poll.get(poll_id)
    if not poll:
        print("Invalid poll.")
        return
    options = poll.options
    votes_for_each_option = [len(option.votes) for  option in options]
    total_votes = sum(votes_for_each_option)
    
    for option, votes in zip(options, votes_for_each_option):
        percentage = float(votes) / total_votes * 100 if total_votes else 0.0
        print(f"{option.text} got {votes} votes ({percentage:.2f}% of total)")


def randomize_poll_winner():
    poll_id = int(input("Enter poll you'd like to pick a winner for: "))
    poll = Poll.get(poll_id)
    if not poll:
        print("Invalid poll.")
        return
    options = poll.options
    _print_poll_options(options)

    option_id = int(input("Enter which is the winning option, we'll pick a random winner from voters: "))
    option = Option.get(option_id)
    if not option:
        print("Invalid option.")
        return
        
    votes = option.votes
    winner = random.choice(votes)
    if winner:
        print(f"The randomly selected winner is {winner[0]}.")
    else:
        print("No votes yet cast for this poll.")

MENU_OPTIONS = {
    "1": prompt_create_poll,
    "2": list_open_polls,
    "3": prompt_vote_poll,
    "4": show_poll_votes,
    "5": randomize_poll_winner
}


def menu():

    connection = create_connection()
    database.create_tables(connection)

    while (selection := input(MENU_PROMPT)) != "6":
        try:
            MENU_OPTIONS[selection]()
        except KeyError:
            print("Invalid input selected. Please try again.")


menu()