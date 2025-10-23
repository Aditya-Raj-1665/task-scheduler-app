import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

function App() {
  const [name, setName] = useState('');
  const [cron, setCron] = useState('*/1 * * * *');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [message, setMessage] = useState(''); 
  const [tasks, setTasks] = useState([]);

  const fetchTasks = async () => {
    try {
      const response = await axios.get('http://localhost:8000/tasks');
      setTasks(response.data); 
    } catch (error) {
      console.error('Error fetching tasks:', error);
      setMessage('Error: Could not fetch tasks from the server.');
    }
  };

  const handleDelete = async (taskId) => {
    try {
      await axios.delete(`http://localhost:8000/tasks/${taskId}`);
      setMessage('Task deleted successfully!');
      fetchTasks(); 
    } catch (error) {
      console.error('Error deleting task:', error);
      setMessage(`Error: ${error.response?.data?.detail || error.message}`);
    }
  };

  const isDateInPast = (dateString) => {
    const endDate = new Date(dateString); 
    const now = new Date();            

    return endDate < now;
  };

  useEffect(() => {
    fetchTasks();
  }, []); 

  const handleSubmit = async (e) => {
    e.preventDefault(); 
    setMessage('Sending...');

    const taskData = {
      name: name,
      cron: cron,
      start_date: new Date(startDate + 'T00:00:00Z').toISOString(),
      end_date: new Date(endDate + 'T23:59:59Z').toISOString(),
    };

    try {
      const response = await axios.post('http://localhost:8000/tasks', taskData);
      console.log('Success:', response.data);
      setMessage(`Success! Task '${name}' created. ${response.data.message || ''}`);
      setName('');
      setCron('*/1 * * * *');
      setStartDate('');
      setEndDate('');
      fetchTasks();
    } catch (error) {
      console.error('Error submitting task:', error);
      setMessage(`Error: ${error.response?.data?.detail || error.message}`);
    }
  };

  return (
    <div className="App" style={{ padding: '20px', maxWidth: '500px', margin: 'auto' }}>
      <h1>Create a New Scheduled Task</h1>
      <form onSubmit={handleSubmit} style={{ display: 'grid', gap: '15px' }}>
        <div>
          <label>Task Name: </label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
          />
        </div>
        <div>
          <label>Cron String: </label>
          <input
            type="text"
            value={cron}
            onChange={(e) => setCron(e.target.value)}
            required
          />
          <small> (e.g., "*/1 * * * *" for every minute)</small>
        </div>
        <div>
          <label>Start Date: </label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            required
          />
        </div>
        <div>
          <label>End Date: </label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            required
          />
        </div>
        <button type="submit">Schedule Task</button>
      </form>

      {message && <p style={{ marginTop: '20px' }}>{message}</p>}

      <h2 style={{ marginTop: '40px' }}>Current Tasks</h2>
      <div className="task-list">
        {tasks.map((task) => {
          const isExpired = isDateInPast(task.end_date);

          return (
            <div key={task._id} style={{
              border: '1px solid #ccc',
              padding: '15px',
              borderRadius: '8px',
              marginBottom: '10px',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              backgroundColor: isExpired ? '#f9f9f9' : '#fff'
            }}>

              <div>
                <strong style={{
                  fontSize: '1.2em',
                  color: isExpired ? '#aaa' : '#000' 
                }}>
                  {task.name}
                  {isExpired && <span style={{ color: '#e74c3c', marginLeft: '10px', fontSize: '0.9em' }}>(Expired)</span>}
                </strong>

                <p style={{ margin: '5px 0 0 0', color: isExpired ? '#ccc' : '#555' }}>
                  Cron: {task.cron}
                </p>
                <p style={{ margin: '5px 0 0 0', color: isExpired ? '#ccc' : '#555' }}>
                  {isExpired ? 'Last Run Was Around: ' : 'Next Run: '}
                  {new Date(task.next_run).toLocaleString()}
                </p>
                <p style={{ margin: '5px 0 0 0', fontSize: '0.8em', color: isExpired ? '#ccc' : '#777' }}>
                  Ends: {new Date(task.end_date).toLocaleDateString()}
                </p>
              </div>

              {/* DELETE BUTTON*/}
              <button
                onClick={() => handleDelete(task._id)}
                onMouseEnter={(e) => (e.target.style.backgroundColor = '#d9534f')}
                onMouseLeave={(e) => (e.target.style.backgroundColor = '#e74c3c')}
                style={{
                  backgroundColor: '#e74c3c',
                  color: 'white',
                  border: 'none',
                  padding: '8px 12px',
                  borderRadius: '5px',
                  cursor: 'pointer'
                }}
              >
                Delete
              </button>

            </div>
          );
        })}
      </div>
    </div>
  );
}

export default App;