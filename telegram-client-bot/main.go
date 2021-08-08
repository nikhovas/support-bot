package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	tgbotapi "github.com/Syfaro/telegram-bot-api"
	"github.com/bykovme/gotrans"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"sync"
	"time"
)

var (
	WarningLogger *log.Logger
	InfoLogger    *log.Logger
	ErrorLogger   *log.Logger
)

type TelegramModule struct {
	bot *tgbotapi.BotAPI
	ampqConn *amqp.Connection
	operatorChannel *amqp.Channel

	QuestionAnswerChannel string
	ProcessQueueName string
	BotToken string
	AmpqUrl string
	OperatorQueue string
	OperatorAnswerChannel string
}

type AnswerMetadata struct {
	ChatId  int64 `json:"chatId"`
	Message int `json:"message"`
}

type Answer struct {
	Question string         `json:"question"`
	Answer   string         `json:"answer"`
	Metadata AnswerMetadata `json:"metadata"`
	Operator bool           `json:"operator"`
}

type NeuralQuestionMessage struct {
	Queue     string          `json:"queue"`
	Metadata  AnswerMetadata `json:"metadata"`
	Question   string `json:"question"`
}

type OperatorQuestionMessage struct {
	NeuralQuestionMessage
	BadAnswer string `json:"badAnswer"`
}

func (tm *TelegramModule) Init() {
	var err error
	tm.bot, err = tgbotapi.NewBotAPI(tm.BotToken)
	if err != nil {
		panic(err)
	} else {
		InfoLogger.Println("Created telegram bot.")
	}

	for i := 0; i < 50; i++ {
		tm.ampqConn, err = amqp.Dial(tm.AmpqUrl)
		if err != nil {
			time.Sleep(10 * time.Second)
			WarningLogger.Println("Can't connect to RMQ. Trying reconnect")
		} else {
			break
		}
	}

	if err != nil {
		panic(err)
	} else {
		InfoLogger.Println("Connected to RMQ.")
	}
}

func (tm *TelegramModule) ReceiveAnswers(stop chan bool, wg *sync.WaitGroup) {
	ch, _ := tm.ampqConn.Channel()
	defer ch.Close()

	q, err := ch.QueueDeclare(tm.QuestionAnswerChannel, false, false, false, false, nil)

	if err != nil {
		ErrorLogger.Println(err)
		return
	}

	msgs, _ := ch.Consume(q.Name, "", true, false, false, false, nil)

loop1:
	for {
		select {
		case d := <-msgs:
			var answer Answer
			InfoLogger.Printf("Got answer: %s\n", string(d.Body))
			err := json.NewDecoder(bytes.NewReader(d.Body)).Decode(&answer)
			if err != nil {
				ErrorLogger.Printf("Can't process: %s\n", d.Body)
				return
			}
			msgText := answer.Answer
			if msgText == "" {
				msgText = gotrans.T("There is no answer for your question yet. The question was sent to our support team")
				answer.Operator = true
				err = tm.operatorChannel.Publish("", tm.OperatorQueue, false, false,
					amqp.Publishing {
						ContentType: "application/json",
						Body:        []byte(fmt.Sprintf(`{"queue": "%s", "metadata": {"chatId": %d, "message": %d}, "question": "%s", "badAnswer": ""}`,
							tm.QuestionAnswerChannel, answer.Metadata.ChatId, answer.Metadata.Message, answer.Question)),
					})
			} else if answer.Operator {
				msgText = "*" + gotrans.T("Operator answer") + "*\n" + msgText
			}

			answerMsg := tgbotapi.NewMessage(answer.Metadata.ChatId, msgText)
			answerMsg.ReplyToMessageID = answer.Metadata.Message
			if !answer.Operator {
				inlineText := fmt.Sprintf("BAD:%d", answer.Metadata.Message)
				answerMsg.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(
					tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(gotrans.T("Bad answer"), inlineText)))
			}
			answerMsg.ParseMode = tgbotapi.ModeMarkdown
			_, _ = tm.bot.Send(answerMsg)
		case _ = <-stop:
			break loop1
		}
	}

	wg.Done()
}

func (tm *TelegramModule) ReceiveMessages(stop chan bool, wg *sync.WaitGroup) {
	ch, err := tm.ampqConn.Channel()
	if err != nil {
		ErrorLogger.Println(err)
		return
	}
	queue, err := ch.QueueDeclare(tm.ProcessQueueName, false, false, false, false, nil)

	tm.operatorChannel, err = tm.ampqConn.Channel()
	if err != nil {
		ErrorLogger.Println(err)
		return
	}
	operatorQueue, err := tm.operatorChannel.QueueDeclare(tm.OperatorQueue, false, false, false, false, nil)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates, _ := tm.bot.GetUpdatesChan(u)
loop2:
	for {
		select {
		case update := <-updates:
			if update.CallbackQuery != nil {
				metadata := AnswerMetadata{
					ChatId:  update.CallbackQuery.Message.Chat.ID,
					Message: update.CallbackQuery.Message.ReplyToMessage.MessageID,
				}
				data := OperatorQuestionMessage{
					NeuralQuestionMessage: NeuralQuestionMessage{
						Queue:    tm.QuestionAnswerChannel,
						Metadata: metadata,
						Question: update.CallbackQuery.Message.ReplyToMessage.Text,
					},
					BadAnswer:             update.CallbackQuery.Message.Text,
				}
				binary, _ := json.Marshal(data)
				InfoLogger.Printf("Bad message: %s\n", string(binary))

				err = tm.operatorChannel.Publish("", operatorQueue.Name, false, false,
					amqp.Publishing {
						ContentType: "application/json",
						Body:        binary,
					})
				editedMessage := tgbotapi.NewEditMessageReplyMarkup(
					update.CallbackQuery.Message.Chat.ID,
					update.CallbackQuery.Message.MessageID,
					tgbotapi.NewInlineKeyboardMarkup(),
					)
				if _, err := tm.bot.Send(editedMessage); err != nil {
					ErrorLogger.Println(err)
				}
			} else if update.Message != nil {
				InfoLogger.Printf("Question: %s\n", update.Message.Text)
				metadata := AnswerMetadata{
					ChatId:  update.Message.Chat.ID,
					Message: update.Message.MessageID,
				}
				data := NeuralQuestionMessage{
					Queue:    tm.QuestionAnswerChannel,
					Metadata: metadata,
					Question: update.Message.Text,
				}
				binary, _ := json.Marshal(data)
				err = ch.Publish("", queue.Name, false, false,
					amqp.Publishing {
						ContentType: "application/json",
						Body:        binary,
					})
			}
		case _ = <-stop:
			break loop2
		}
	}

	wg.Done()
}

func (tm *TelegramModule) Run() {
	st := make(chan bool, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go tm.ReceiveMessages(st, &wg)
	go tm.ReceiveAnswers(st, &wg)
	wg.Wait()
}

func GetEnvOrDefault(key, defaultValue string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	return val
}

func main() {
	InfoLogger = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WarningLogger = log.New(os.Stderr, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	_ = gotrans.InitLocales("./messages")
	_ = gotrans.SetDefaultLocale("ru")

	tm := TelegramModule{
		BotToken: GetEnvOrDefault("TG_CLIENT_BOT_TOKEN", ""),
		ProcessQueueName: GetEnvOrDefault("NEURAL_QUESTIONS_QUEUE", "neural_questions"),
		QuestionAnswerChannel: "telegram_client",
		OperatorQueue: GetEnvOrDefault("OPERATOR_QUESTION_QUEUE", "operator_questions"),
		AmpqUrl: GetEnvOrDefault("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
	}

	tm.Init()
	tm.Run()
}
