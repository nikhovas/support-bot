import pika
import json
import nltk
import ssl
import redis
import logging
from nltk.corpus import stopwords
from string import punctuation
import gensim.downloader as api
import pymorphy2


try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
nltk.download("stopwords")


punc = punctuation + '–'
russian_stopwords = stopwords.words("russian")
model = api.load('word2vec-ruscorpora-300')
morph = pymorphy2.MorphAnalyzer()
tag_fix = {'ADJF': 'ADJ', 'ADJS': 'ADJ', 'INFN': 'VERB', 'PRED': 'ADV', 'ADVB': 'ADV', 'CONJ': 'SCONJ'}


def pp(sentence):
    for c in punc:
        sentence = sentence.replace(c, ' ')
    result = []
    for w in sentence.lower().split():
        if w not in russian_stopwords:
            p = morph.parse(w)[0]
            nf = p.normal_form
            pos = p.tag.POS if p.tag.POS is not None else ''
            if pos in set(tag_fix.keys()):
                pos = tag_fix[pos]
            result.append(nf + '_' + pos)
    return result


def getmetric_q(question, q):
    pp_ = pp(question)
    return model.wmdistance(pp_, q.characteristic) / (1 + 0.1 * len(set(pp_) & set(q.characteristic)))


class Question:
    def __init__(self, q, a, characteristics=None):
        self.q = q
        self.a = a
        if characteristics is None:
            self.characteristic = []
            self.characteristic += pp(q)
            self.characteristic += pp(a)
        else:
            self.characteristic = characteristics


def wrap_to_redis_value(key: str, value: str) -> str:
    return value + '    ' + ' '.join(pp(key) + pp(value))


def wrap_to_answer(key: str, redis_value: str) -> Question:
    arr1 = redis_value.split('    ')
    answer = arr1[0]
    characteristics = arr1[1].split(' ')
    return Question(key, answer, characteristics)


questions = [
    Question(
        'Какое минимальное количество баллов нужно набрать при сдаче TOEFL, чтобы сертификат был учтен в ИД?',
        'Здравствуйте! Ограничений нет. Баллы добавляется за наличие сертификата, полученного не ранее 2018 года.'
    ),
    Question(
        'Можно ли претендовать на поступление на бюджет в магистратуру МФТИ, если я окончила бакалавриат по '
        'гуманитарному направлению другого ВУЗа? ',
        'Добрый день! Вы можете претендовать на бюджет. Успех зависит от сдачи вступительных испытаний и баллов за '
        'индивидуальные достижения. '
    )
]


