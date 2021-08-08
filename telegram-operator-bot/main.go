package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	tgbotapi "github.com/Syfaro/telegram-bot-api"
	"github.com/bykovme/gotrans"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var (
	WarningLogger *log.Logger
	InfoLogger    *log.Logger
	ErrorLogger   *log.Logger

	GetQuestionButtonText     string
)

type OperatorState struct {
	State    int
	CurrentStateCounter int
	Mutex    sync.Mutex
	ChatId    int64

	DeliveryTag      uint64
	OperatorQuestion OperatorQuestion
}

const (
	InitialState = iota
	AuthState    = iota
	WaitForQuestionState = iota
	WaitForAnswerState = iota
)

type TelegramModule struct {
	bot                       *tgbotapi.BotAPI
	ampqConn                  *amqp.Connection
	db                        *sql.DB
	operatorQuestionChannel   *amqp.Channel
	databaseQuestionAddChannel *amqp.Channel
	operatorQuestions          <-chan amqp.Delivery
	operators                 map[int64]*OperatorState

	BotToken                  string
	OperatorQuestionQueueName string
	AmpqUrl                   string
	DatabaseQuestionAddQueue  string
	OperatorAnswerChannel     string
	QuestionAnswerTimeout     int
	InactivityTimeout         int

	DatabaseUrl               string
	DatabasePort              string
	DatabaseUser              string
	DatabasePassword          string
	DatabaseName              string
}

type OperatorQuestion struct {
	Queue     string      `json:"queue"`
	Metadata  interface{} `json:"metadata"`
	Question  string      `json:"question"`
	BadAnswer string      `json:"badAnswer"`
}

func (tm *TelegramModule) Init() {
	var err error
	tm.bot, err = tgbotapi.NewBotAPI(tm.BotToken)
	if err != nil {
		panic(err)
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
	}

	tm.operators = make(map[int64]*OperatorState)
}

func (tm *TelegramModule) SendSimpleMessage(chatId int64, text string) {
	msg := tgbotapi.NewMessage(chatId, text)
	if _, err := tm.bot.Send(msg); err != nil {
		ErrorLogger.Println(err)
	}
}

func (tm *TelegramModule) WaitForQuestionAnswerTimeout(state *OperatorState, currentStateCounter int) {
	time.Sleep(time.Duration(tm.QuestionAnswerTimeout) * time.Second)
	state.Mutex.Lock()
	defer state.Mutex.Unlock()
	if state.CurrentStateCounter == currentStateCounter && state.State == WaitForAnswerState {
		if err := tm.operatorQuestionChannel.Nack(state.DeliveryTag, false, true); err != nil {
			ErrorLogger.Println(err)
		}
		tm.SendSimpleMessage(state.ChatId, gotrans.T("Timeout for question answering reached"))
		tm.ToWaitForQuestionState(state)
	}
}

func (tm *TelegramModule) WaitForInactivityDelete(state *OperatorState, currentStateCounter int) {
	for {
		time.Sleep(time.Duration(tm.InactivityTimeout) * time.Second)
		state.Mutex.Lock()
		if state.CurrentStateCounter == currentStateCounter {
			tm.SendSimpleMessage(state.ChatId, gotrans.T("Inactivity timeout. Please, auth again to use the bot."))
			delete(tm.operators, state.ChatId)
			state.Mutex.Unlock()
			break
		}
		currentStateCounter = state.CurrentStateCounter
		state.Mutex.Unlock()
	}
}

func (tm *TelegramModule) ToAuthState(state *OperatorState) {
	msg := tgbotapi.NewMessage(state.ChatId, gotrans.T("Auth via phone number"))
	var keyboard = tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButtonContact(gotrans.T("Send phone")),
		),
	)
	msg.ReplyMarkup = keyboard
	if _, err := tm.bot.Send(msg); err != nil {
		ErrorLogger.Println(err)
		return
	}

	state.State = AuthState
	state.CurrentStateCounter++
}

func (tm *TelegramModule) FromAuthState(update *tgbotapi.Update, state *OperatorState) {
	phone := update.Message.Contact.PhoneNumber
	if update.Message == nil || update.Message.Contact == nil {
		tm.ToAuthState(state)
		return
	}
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		ErrorLogger.Println(err)
		return
	}
	phone = reg.ReplaceAllString(phone, "")

	rows, err := tm.db.Query("SELECT 1 FROM operator WHERE phone = '" + phone + "';")
	if err != nil {
		ErrorLogger.Println(err)
		return
	}
	exists := rows.Next()
	_ = rows.Close()
	if exists {
		tm.ToWaitForQuestionState(state)
	} else {
		tm.SendSimpleMessage(state.ChatId, gotrans.T("You do not have permission to use this bot"))
	}
}

func (tm *TelegramModule) FromInitialState(state *OperatorState) {
	tm.ToAuthState(state)
}

func (tm *TelegramModule) ToWaitForQuestionState(state *OperatorState) {
	msg := tgbotapi.NewMessage(state.ChatId, gotrans.T("Press button to get question"))
	msg.ReplyMarkup = tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(tgbotapi.NewKeyboardButton(GetQuestionButtonText)),
	)
	if _, err := tm.bot.Send(msg); err != nil {
		ErrorLogger.Println(err)
	}

	state.State = WaitForQuestionState
	state.CurrentStateCounter++
	state.OperatorQuestion = OperatorQuestion{}
}

func (tm *TelegramModule) FromWaitForQuestionState(update *tgbotapi.Update, state *OperatorState) {
	text := update.Message.Text
	if text == GetQuestionButtonText {
		t1 := time.Tick(time.Second) // timer
		select {
		case <-t1:
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, gotrans.T("No questions yet"))
			_, err := tm.bot.Send(msg)
			if err != nil {
				ErrorLogger.Println(err)
				return
			}
		case d := <-tm.operatorQuestions:
			if d.Body == nil {
				if err := d.Ack(false); err != nil {
					ErrorLogger.Println(err)
				}
				return
			}

			var operatorQuestion OperatorQuestion
			stringQuestion := string(d.Body)
			InfoLogger.Println(stringQuestion)
			if err := json.Unmarshal(d.Body, &operatorQuestion); err != nil {
				ErrorLogger.Println(err)
				if err := d.Ack(false); err != nil {
					ErrorLogger.Println(err)
				}
				tm.SendSimpleMessage(state.ChatId, gotrans.T("Error while getting question"))
				tm.ToWaitForQuestionState(state)
				return
			}
			state.DeliveryTag = d.DeliveryTag
			state.OperatorQuestion = operatorQuestion
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, operatorQuestion.Question)
			msg.ReplyMarkup = tgbotapi.NewRemoveKeyboard(false)
			_, err := tm.bot.Send(msg)
			if err != nil {
				ErrorLogger.Println(err)
			}

			state.State = WaitForAnswerState
			state.CurrentStateCounter++
			go tm.WaitForQuestionAnswerTimeout(state, state.CurrentStateCounter)
		}
	}
}

func (tm *TelegramModule) FromWaitForAnswerState(update *tgbotapi.Update, state *OperatorState) {
	defer tm.ToWaitForQuestionState(state)

	answer := update.Message.Text
	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte(fmt.Sprintf(`{"question": "%s", "answer": "%s"}`, state.OperatorQuestion.Question, answer)),
	}

	if err := tm.databaseQuestionAddChannel.Publish("", tm.DatabaseQuestionAddQueue, false,
		false, publishing); err != nil {
		ErrorLogger.Println(err)
		return
	}

	if err := tm.operatorQuestionChannel.Ack(state.DeliveryTag, false); err != nil {
		ErrorLogger.Println(err)
		return
	}

	answerChannel, err := tm.ampqConn.Channel()
	if err != nil {
		ErrorLogger.Println(err)
		return
	}
	answerQueue, err := answerChannel.QueueDeclare(
		state.OperatorQuestion.Queue, false, false, false, false, nil)
	if err != nil {
		ErrorLogger.Println(err)
		return
	}

	metadataBytes, err := json.Marshal(state.OperatorQuestion.Metadata)
	metadataStr := string(metadataBytes)

	publishing = amqp.Publishing{
		ContentType:     "application/json",
		Body:            []byte(fmt.Sprintf(`{"metadata": %s, "answer": "%s", "operator": true}`, metadataStr, answer)),
	}
	if err := answerChannel.Publish("", answerQueue.Name, false, false, publishing); err != nil {
		ErrorLogger.Println(err)
	}
}

func (tm *TelegramModule) ManageUpdate(update *tgbotapi.Update) {
	var chatId int64 = 0
	if update.Message != nil {
		if update.Message.Chat != nil {
			chatId = update.Message.Chat.ID
		}
	}

	operatorState, ok := tm.operators[chatId]
	if !ok {
		tm.operators[chatId] = &OperatorState{State: InitialState, ChatId: chatId}
		operatorState = tm.operators[chatId]
		go tm.WaitForInactivityDelete(operatorState, operatorState.CurrentStateCounter + 1)
	}

	operatorState.Mutex.Lock()
	defer operatorState.Mutex.Unlock()

	switch operatorState.State {
	case InitialState:
		tm.FromInitialState(operatorState)
		break
	case AuthState:
		tm.FromAuthState(update, operatorState)
		break
	case WaitForQuestionState:
		tm.FromWaitForQuestionState(update, operatorState)
		break
	case WaitForAnswerState:
		tm.FromWaitForAnswerState(update, operatorState)
		break
	default:
		break
	}
}

func (tm *TelegramModule) Run(stop chan bool) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		tm.DatabaseUrl, tm.DatabasePort, tm.DatabaseUser, tm.DatabasePassword, tm.DatabaseName)
	var err error
	tm.db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		ErrorLogger.Println(err)
		return
	}

	tm.operatorQuestionChannel, err = tm.ampqConn.Channel()
	if err != nil {
		ErrorLogger.Println(err)
		return
	}
	if _, err = tm.operatorQuestionChannel.QueueDeclare(
		tm.OperatorQuestionQueueName, false, false, false, false, nil); err != nil {
		ErrorLogger.Println(err)
		return
	}

	tm.operatorQuestions, err = tm.operatorQuestionChannel.Consume(
		tm.OperatorQuestionQueueName, "", false, false, false, false, nil)
	if err != nil {
		ErrorLogger.Println(err)
		return
	}

	tm.databaseQuestionAddChannel, err = tm.ampqConn.Channel()
	if err != nil {
		ErrorLogger.Println(err)
		return
	}

	if _, err = tm.databaseQuestionAddChannel.QueueDeclare(
		tm.DatabaseQuestionAddQueue, false, false, false, false, nil); err != nil {
		return
	}

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates, _ := tm.bot.GetUpdatesChan(u)
loop:
	for {
		select {
		case update := <-updates:
			go tm.ManageUpdate(&update)
		case _ = <-stop:
			_ = tm.db.Close()
			_ = tm.operatorQuestionChannel.Close()
			_ = tm.databaseQuestionAddChannel.Close()
			break loop
		}
	}
}

func GetEnvOrDefault(key, defaultValue string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	return val
}

func main() {
	_ = gotrans.InitLocales("./messages")
	_ = gotrans.SetDefaultLocale("ru")
	GetQuestionButtonText = gotrans.T("Get Question")

	InfoLogger = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WarningLogger = log.New(os.Stderr, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	questionAnswerTimeout, err := strconv.Atoi(GetEnvOrDefault("TG_OPERATOR_BOT_QUESTION_ANSWER_TIMEOUT", "10"))
	if err != nil {
		ErrorLogger.Fatal(err)
	}

	inactivityTimeout, err := strconv.Atoi(GetEnvOrDefault("TG_OPERATOR_BOT_INACTIVITY_TIMEOUT", "10"))
	if err != nil {
		ErrorLogger.Fatal(err)
	}

	tm := TelegramModule{
		BotToken:                  GetEnvOrDefault("TG_OPERATOR_BOT_TOKEN", ""),
		OperatorQuestionQueueName: GetEnvOrDefault("OPERATOR_QUESTION_QUEUE", "operator_questions"),
		DatabaseQuestionAddQueue:  GetEnvOrDefault("TO_DATABASE_QUEUE", "database_add"),
		AmpqUrl:                   GetEnvOrDefault("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		QuestionAnswerTimeout:     questionAnswerTimeout,
		InactivityTimeout:         inactivityTimeout,

		DatabaseUrl:               GetEnvOrDefault("DATABASE_URL", "localhost"),
		DatabasePort:              GetEnvOrDefault("DATABASE_PORT", "5432"),
		DatabaseUser:              GetEnvOrDefault("DATABASE_USER", "postgres"),
		DatabasePassword:          GetEnvOrDefault("DATABASE_PASSWORD", "postgres"),
		DatabaseName:              GetEnvOrDefault("DATABASE_NAME", "support_bot"),
	}

	tm.Init()
	st := make(chan bool, 1)
	tm.Run(st)
}