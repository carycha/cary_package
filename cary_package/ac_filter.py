import json
import ahocorasick


class ACFilter:
    def __init__(self):
        self.ac_machine = ahocorasick.Automaton()

    def add_word_tuple_list(self, word_tuple_list):
        # input sample [("澳門賭城",{"name":"澳門賭城"，"category":"gamble"}),()]
        for word_tuple in word_tuple_list:
            self.ac_machine.add_word(word_tuple[0], word_tuple[1])
        self.ac_machine.make_automaton()
        # return self.ac_machine

    def iter_long(self, text):
        found_tags = list(self.ac_machine.iter_long(text))
        return found_tags
