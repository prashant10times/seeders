package mail

import (
	"bytes"
	"fmt"
	"html/template"
	"time"
)

const baseTemplate = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f4f6f8;
            padding: 20px;
        }
        .container {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
        }
        .header {
            font-size: 20px;
            font-weight: bold;
        }
        .status {
            margin-top: 10px;
            font-size: 16px;
        }
        .details {
            margin-top: 12px;
            font-size: 14px;
            white-space: pre-wrap;
            word-break: break-word;
            background: #f8f9fa;
            padding: 12px;
            border-radius: 4px;
            border: 1px solid #e9ecef;
        }
        .footer {
            margin-top: 20px;
            font-size: 12px;
            color: #888;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">{{.Title}}</div>
        <div class="status">{{.Message}}</div>
        {{if .Extra}}
        <div class="details"><strong>Details:</strong> {{.Extra}}</div>
        {{end}}
        <div class="footer">
            Time: {{.Time}}
        </div>
    </div>
</body>
</html>
`

type TemplateData struct {
	Title   string
	Message string
	Extra   string
	Time    string
}

func renderSeedingEmailHTML(data TemplateData) (string, error) {
	t, err := template.New("seeding-email").Parse(baseTemplate)
	if err != nil {
		return "", err
	}
	var body bytes.Buffer
	if err := t.Execute(&body, data); err != nil {
		return "", err
	}
	return body.String(), nil
}

func SeedingStartedTemplate(jobName string) (subject string, body string, err error) {
	data := TemplateData{
		Title:   "Seeding started",
		Message: fmt.Sprintf("Seeding process %s has started.", jobName),
		Time:    time.Now().Format(time.RFC1123),
	}
	html, err := renderSeedingEmailHTML(data)
	return "Seeding started", html, err
}

func SeedingErrorTemplate(jobName string, errorMsg string) (subject string, body string, err error) {
	data := TemplateData{
		Title:   "Seeding failed",
		Message: fmt.Sprintf("Seeding process %s encountered an error.", jobName),
		Extra:   errorMsg,
		Time:    time.Now().Format(time.RFC1123),
	}
	html, err := renderSeedingEmailHTML(data)
	return "Seeding failed", html, err
}

func SeedingCompletedTemplate(jobName string, duration string) (subject string, body string, err error) {
	data := TemplateData{
		Title:   "Seeding completed",
		Message: fmt.Sprintf("Seeding process %s completed successfully.", jobName),
		Extra:   fmt.Sprintf("Duration: %s", duration),
		Time:    time.Now().Format(time.RFC1123),
	}
	html, err := renderSeedingEmailHTML(data)
	return "Seeding completed", html, err
}