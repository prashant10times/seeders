package mail

import (
	"fmt"
	"os"
	"strings"

	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

func newFromEmail() *mail.Email {
	name := strings.TrimSpace(os.Getenv("SENDGRID_FROM_NAME"))
	if name == "" {
		name = "Microservice Seeder"
	}
	addr := strings.TrimSpace(os.Getenv("SENDGRID_FROM_EMAIL"))
	if addr == "" {
		addr = "no-replyseeder@10times.com"
	}
	return mail.NewEmail(name, addr)
}

type EmailRequest struct {
	To      string
	Subject string
	Body    string
}

type BulkEmailRequest struct {
	ToList  []string
	Subject string
	Body    string
}

type HTMLEmailRequest struct {
	To        string
	Subject   string
	PlainText string
	HTML      string
}

func SendHTMLEmail(req HTMLEmailRequest) error {
	from := newFromEmail()
	to := mail.NewEmail("", req.To)
	plain := req.PlainText
	if plain == "" {
		plain = "This message requires an HTML-capable email client."
	}
	message := mail.NewSingleEmail(from, req.Subject, to, plain, req.HTML)
	client := sendgrid.NewSendClient(os.Getenv("SENDGRID_API_KEY"))
	response, err := client.Send(message)
	if err != nil {
		return err
	}
	if response.StatusCode >= 400 {
		return fmt.Errorf("failed with status code: %d", response.StatusCode)
	}
	return nil
}

// Send Single Email
func SendEmail(req EmailRequest) error {
	from := newFromEmail()
	to := mail.NewEmail("", req.To)

	message := mail.NewSingleEmail(
		from,
		req.Subject,
		to,
		req.Body,
		req.Body,
	)

	client := sendgrid.NewSendClient(os.Getenv("SENDGRID_API_KEY"))
	response, err := client.Send(message)

	if err != nil {
		return err
	}

	if response.StatusCode >= 400 {
		return fmt.Errorf("failed with status code: %d", response.StatusCode)
	}

	return nil
}

// Send Bulk Email
func SendBulkEmail(req BulkEmailRequest) error {
	from := newFromEmail()

	m := mail.NewV3Mail()
	m.SetFrom(from)
	m.Subject = req.Subject

	p := mail.NewPersonalization()

	for _, recipient := range req.ToList {
		p.AddTos(mail.NewEmail("", recipient))
	}

	m.AddPersonalizations(p)

	content := mail.NewContent("text/plain", req.Body)
	m.AddContent(content)

	client := sendgrid.NewSendClient(os.Getenv("SENDGRID_API_KEY"))
	response, err := client.Send(m)

	if err != nil {
		return err
	}

	if response.StatusCode >= 400 {
		return fmt.Errorf("failed with status code: %d", response.StatusCode)
	}

	return nil
}