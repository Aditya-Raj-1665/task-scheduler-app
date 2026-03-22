import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Play, Pause, Square, Activity, XCircle, Clock, CheckCircle, BarChart2 } from 'lucide-react';
import './App.css';

const API_BASE = 'http://localhost:8000';

function App() {
  const [activeTab, setActiveTab] = useState('pending');
  const [tasks, setTasks] = useState([]);
  const [message, setMessage] = useState('');
  
  const [name, setName] = useState('');
  const [cron, setCron] = useState('*/1 * * * *');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [priority, setPriority] = useState(3);
  const [maxRetries, setMaxRetries] = useState(3);
  const [scriptCode, setScriptCode] = useState('class CustomOperator(BaseOperator):\n    def initialize(self):\n        pass\n\n    def run(self):\n        print("Running custom task")\n\n    def finish(self):\n        pass');

  const fetchTasks = async () => {
    try {
      const response = await axios.get(`${API_BASE}/tasks`);
      setTasks(response.data);
    } catch (error) {
      console.error('Error fetching tasks:', error);
    }
  };

  useEffect(() => {
    fetchTasks();
    const interval = setInterval(fetchTasks, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setMessage('Creating task...');
    try {
      const taskData = {
        task_name: name,
        cron,
        start_date: new Date(startDate || Date.now()).toISOString(),
        end_date: new Date(endDate || Date.now() + 86400000000).toISOString(), /* Default generous end date */
        priority: parseInt(priority, 10),
        max_retries: parseInt(maxRetries, 10),
        task_config: { 
            operator_path: 'worker/operators/example_operator.py',
            payload: { task_name: name, custom_code: scriptCode },
            connection: {}
        }
      };
      await axios.post(`${API_BASE}/tasks`, taskData);
      setMessage('Task created successfully!');
      fetchTasks();
      setTimeout(() => setMessage(''), 3000);
    } catch (error) {
      setMessage('Error creating task');
    }
  };

  const handleAction = async (taskName, action) => {
    try {
      await axios.post(`${API_BASE}/tasks/${taskName}/${action}`);
      fetchTasks();
    } catch (error) {
      alert('Failed action');
    }
  };

  const formatState = (state) => state || 'PENDING';

  const filterTasks = (tab) => {
    return tasks.filter(task => {
        const state = formatState(task.state);
        switch (tab) {
           case 'pending': return ['PENDING', 'PAUSED'].includes(state);
           case 'running': return state === 'RUNNING';
           case 'completed': return ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'INVALID', 'EXHAUSTED'].includes(state);
           default: return true;
        }
    });
  };

  const totalTasks = tasks.length;
  const runningTasks = tasks.filter(t => formatState(t.state) === 'RUNNING').length;
  const failedTasks = tasks.filter(t => ['FAILED', 'INVALID', 'EXHAUSTED', 'TIMED_OUT'].includes(formatState(t.state))).length;
  const completedTasks = tasks.filter(t => formatState(t.state) === 'COMPLETED').length;

  return (
    <div className="App">
      <header className="app-header">
        <h2><Activity className="header-icon" size={32} /> Scheduler Control</h2>
      </header>

      {message && <div className="alert">{message}</div>}

      <div className="analytics-row">
        <div className="stat-card">
          <div className="stat-icon"><BarChart2 /></div>
          <div className="stat-details">
            <h3>Total Indexed</h3>
            <p>{totalTasks}</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon pulse-icon"><Activity /></div>
          <div className="stat-details">
            <h3>Active Runs</h3>
            <p>{runningTasks}</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon success-icon"><CheckCircle /></div>
          <div className="stat-details">
            <h3>Completed</h3>
            <p>{completedTasks}</p>
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-icon error-icon"><XCircle /></div>
          <div className="stat-details">
            <h3>Failures</h3>
            <p>{failedTasks}</p>
          </div>
        </div>
      </div>

      <div className="glass-panel form-container">
        <h3>Create New Mission</h3>
        <form onSubmit={handleSubmit} className="split-form">
          <div className="form-left">
              <input type="text" placeholder="Mission Name Identifier" value={name} onChange={e => setName(e.target.value)} required />
              <input type="text" placeholder="Cron Sequence (e.g. */1 * * * *)" value={cron} onChange={e => setCron(e.target.value)} required />
              <div className="date-group">
                <label>Commencement Date</label>
                <input type="datetime-local" value={startDate} onChange={e => setStartDate(e.target.value)} required />
              </div>
              <div className="date-group">
                <label>Termination Date</label>
                <input type="datetime-local" value={endDate} onChange={e => setEndDate(e.target.value)} required />
              </div>
              <div className="number-group">
                <input type="number" min="1" max="3" value={priority} title="Priority (1-3)" onChange={e => setPriority(e.target.value)} />
                <input type="number" min="0" max="5" value={maxRetries} title="Failure Retries" onChange={e => setMaxRetries(e.target.value)} />
              </div>
              <button type="submit" className="submit-btn"><Play size={16} /> Deploy Mission Code</button>
          </div>
          <div className="form-right">
             <label>Live Operator Code Payload (Python 3)</label>
             <textarea 
                className="code-editor" 
                value={scriptCode} 
                onChange={e => setScriptCode(e.target.value)}
                spellCheck="false"
             />
          </div>
        </form>
      </div>

      <div className="tabs">
        <button className={activeTab === 'pending' ? 'active' : ''} onClick={() => setActiveTab('pending')}>Queue ({filterTasks('pending').length})</button>
        <button className={activeTab === 'running' ? 'active' : ''} onClick={() => setActiveTab('running')}>Executing ({runningTasks})</button>
        <button className={activeTab === 'completed' ? 'active' : ''} onClick={() => setActiveTab('completed')}>History ({filterTasks('completed').length})</button>
      </div>

      <div className="mission-cards-grid">
        {(filterTasks(activeTab) || []).map(t => (
          <div className="mission-card" key={t.task_name}>
            <div className="card-header">
              <span className="task-name">{t.task_name}</span>
              <div className={`status-badge status-${formatState(t.state).toLowerCase()}`}>
                {formatState(t.state) === 'RUNNING' && <span className="pulse-dot"></span>}
                {formatState(t.state)}
              </div>
            </div>
            
            <div className="card-body">
              <div className="card-meta">
                <Clock size={14} /> 
                <span>{t.next_run ? new Date(t.next_run).toLocaleString() : 'Offline'}</span>
              </div>
              <div className="card-meta">
                <Activity size={14} /> 
                <span>Priority {t.priority || 3}  |  Retries: {t.max_retries || 0}</span>
              </div>
            </div>

            <div className="card-actions">
              <button className="action-btn icon-btn" onClick={() => handleAction(t.task_name, 'run')} title="Start">
                <Play size={16} />
              </button>
              {formatState(t.state) === 'PAUSED' ? (
                 <button className="action-btn icon-btn" onClick={() => handleAction(t.task_name, 'resume')} title="Resume">
                    <Play size={16} />
                 </button>
              ) : (
                 <button className="action-btn icon-btn" onClick={() => handleAction(t.task_name, 'pause')} title="Pause">
                    <Pause size={16} />
                 </button>
              )}
              <button className="action-btn icon-btn stop-btn" onClick={() => handleAction(t.task_name, 'cancel')} title="Terminate">
                <Square size={16} />
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default App;
