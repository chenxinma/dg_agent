// BIG FAT WARNING: to avoid the complexity of npm, this typescript is compiled in the browser
// there's currently no static type checking

import { marked } from 'https://cdn.bootcdn.net/ajax/libs/marked/15.0.6/lib/marked.esm.min.js'
const convElement = document.getElementById('conversation')

const promptInput = document.getElementById('prompt-input') as HTMLInputElement
const spinner = document.getElementById('spinner')
const stopButton = document.getElementById('stop-button') as HTMLButtonElement

// stream the response and render messages as each chunk is received
// data is sent as newline-delimited JSON
async function onFetchResponse(response: Response): Promise<void> {
  let text = ''
  let decoder = new TextDecoder()
  if (response.ok) {
    if (!response.body) {
      console.warn('Response body is null, skipping streaming.');
      return;
    }
    if (stopButton) {
      stopButton.classList.remove('d-none');
    }
    const reader = response.body.getReader()
    while (true) {
      const {done, value} = await reader.read()
      if (done) {
        break
      }
      text += decoder.decode(value)
      addMessages(text)
      if (spinner) {
        spinner.classList.remove('active')
      }
      // 如果接收到新的消息则重置控制器
      controller = new AbortController()
    }
    addMessages(text)
    promptInput.disabled = false
    promptInput.focus()
    if (stopButton) {
      stopButton.classList.add('d-none');
    }
  } else {
    const text = await response.text()
    console.error(`Unexpected response: ${response.status}`, {response, text})
    throw new Error(`Unexpected response: ${response.status}`)
  }
}

// The format of messages, this matches pydantic-ai both for brevity and understanding
// in production, you might not want to keep this format all the way to the frontend
interface Message {
  role: string
  content: string
  timestamp: string
}

// take raw response text and render messages into the `#conversation` element
// Message timestamp is assumed to be a unique identifier of a message, and is used to deduplicate
// hence you can send data about the same message multiple times, and it will be updated
// instead of creating a new message elements
function addMessages(responseText: string) {
  const lines = responseText.split('\n')
  const messages: Message[] = lines.filter(line => line.length > 1).map(j => JSON.parse(j))
  for (const message of messages) {
    // we use the timestamp as a crude element id
    const {timestamp, role, content} = message
    const id = `msg-${timestamp}`
    let msgDiv = document.getElementById(id)
    if (!msgDiv) {
      msgDiv = document.createElement('div')
      msgDiv.id = id
      msgDiv.title = `${role} at ${timestamp}`
      msgDiv.classList.add('border-top', 'pt-2', role)
      if (convElement) {
        convElement.appendChild(msgDiv)
      }
    }
    msgDiv.innerHTML = marked.parse(content)
  }
  window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' })
}

function onError(error: any) {
  console.error(error)
  let _err = document.getElementById('error')
  if (_err) {
    _err.classList.remove('d-none')
  }
  let _spinner = document.getElementById('spinner')
  if (_spinner) {
    _spinner.classList.remove('active')
  }
}

let controller: AbortController | null = null

async function onSubmit(e: SubmitEvent): Promise<void> {
  e.preventDefault()
  if (spinner) {
    spinner.classList.add('active')
  }
  const body = new FormData(e.target as HTMLFormElement)
  
  promptInput.value = ''
  promptInput.disabled = true
  
  // 使用AbortController来控制fetch请求
  controller = new AbortController()
  const response = await fetch('/chat/', { method: 'POST', body, signal: controller.signal })
  await onFetchResponse(response)
}

  // 添加停止按钮的事件监听器
  if (stopButton) {
    stopButton.addEventListener('click', () => {
      if (controller) {
        controller.abort()
        console.log('Fetch aborted.')
        if (spinner) {
          spinner.classList.remove('active')
        }
        promptInput.disabled = false
      }
    })
  }

  // call onSubmit when the form is submitted (e.g. user clicks the send button or hits Enter)
  let _form = document.querySelector('form')
  if (_form) {
    _form.addEventListener('submit', (e) => onSubmit(e).catch(onError))
  }
  
  // load messages on page load
  fetch('/chat/').then(onFetchResponse).catch(onError)
