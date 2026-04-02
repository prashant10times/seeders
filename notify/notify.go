package notify

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"seeders/mail"
)

func recipientsFromEnv(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func Recipients() []string {
	return recipientsFromEnv("SEEDING_NOTIFY_EMAIL")
}

func ErrorRecipients() []string {
	return recipientsFromEnv("SEEDING_ERROR_NOTIFY_EMAIL")
}

func JobName(defaultName string) string {
	if s := strings.TrimSpace(os.Getenv("SEEDING_JOB_NAME")); s != "" {
		return s
	}
	return defaultName
}

func apiReady() bool {
	return strings.TrimSpace(os.Getenv("SENDGRID_API_KEY")) != ""
}

func sendHTMLEmailToRecipients(recipients []string, subject, plainText, html string) {
	for _, to := range recipients {
		if err := mail.SendHTMLEmail(mail.HTMLEmailRequest{To: to, Subject: subject, PlainText: plainText, HTML: html}); err != nil {
			log.Printf("seeding notify: send to %q: %v", to, err)
		}
	}
}

func TryStarted(recipients []string, jobName string) {
	if len(recipients) == 0 || !apiReady() {
		return
	}
	sub, html, err := mail.SeedingStartedTemplate(jobName)
	if err != nil {
		log.Printf("seeding notify: build started template: %v", err)
		return
	}
	plain := fmt.Sprintf("Seeding %s has started.", jobName)
	sendHTMLEmailToRecipients(recipients, sub, plain, html)
}

func TryCompleted(recipients []string, jobName string, elapsed time.Duration) {
	if len(recipients) == 0 || !apiReady() {
		return
	}
	sub, html, err := mail.SeedingCompletedTemplate(jobName, elapsed.String())
	if err != nil {
		log.Printf("seeding notify: build completed template: %v", err)
		return
	}
	plain := fmt.Sprintf("Seeding %s completed successfully in %s.", jobName, elapsed)
	sendHTMLEmailToRecipients(recipients, sub, plain, html)
}

func Fatalf(notifyRecipients []string, jobName, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	errTo := ErrorRecipients()
	if len(errTo) == 0 {
		errTo = notifyRecipients
	}
	if len(errTo) > 0 && apiReady() {
		sub, html, err := mail.SeedingErrorTemplate(jobName, msg)
		if err != nil {
			log.Printf("seeding notify: build error template: %v", err)
		} else {
			sendHTMLEmailToRecipients(errTo, sub, msg, html)
		}
	}
	log.Fatalf(format, args...)
}