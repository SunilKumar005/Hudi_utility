import React, { useState } from 'react';
import axios from 'axios';
import {
    TextField, Button, Chip, FormControl, CircularProgress,
    Typography, Box, Alert, Container, Grid, Stack,
    Select, MenuItem, InputLabel
} from '@mui/material';

function App() {
    const [formData, setFormData] = useState({
        data_file_path: "",
        hudi_table_name: "",
        key_field: [],
        precombine_field: "",
        partition_field: "",
        hudi_table_type: "",
        write_operation: "",
        output_path: "",
        spark_config: { 'spark.executor.memory': '2g' },
        schema_validation: false,
        dry_run: false,
        bootstrap_type: "",
        partition_regex: ""
    });

    const [loading, setLoading] = useState(false);
    const [log, setLog] = useState('');
    const [logModalOpen, setLogModalOpen] = useState(false);
    const [message, setMessage] = useState({ text: '', severity: '' });
    const [showLogs, setShowLogs] = useState(false);
    const [newKeyField, setNewKeyField] = useState("");

    const handleChange = (e) => {
        const { name, value, type, checked } = e.target;
        if (type === 'checkbox') {
            setFormData(prevFormData => ({ ...prevFormData, [name]: checked }));
        } else {
            setFormData(prevFormData => ({ ...prevFormData, [name]: value }));
        }
    };

    const handleKeyFieldChange = (e) => {
        setNewKeyField(e.target.value);
    };

    const handleAddKeyField = () => {
        if (newKeyField && !formData.key_field.includes(newKeyField)) {
            setFormData(prevFormData => ({
                ...prevFormData,
                key_field: [...prevFormData.key_field, newKeyField]
            }));
            setNewKeyField("");
        }
    };

    const handleDeleteKeyField = (field) => {
        setFormData(prevFormData => ({
            ...prevFormData,
            key_field: prevFormData.key_field.filter(key => key !== field)
        }));
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setMessage({ text: '', severity: '' });
        setShowLogs(false);
        try {
            const response = await axios.post("http://127.0.0.1:8000/bootstrap_hudi/", formData, {
                headers: { 'Content-Type': 'application/json' }
            });
            if (response.status === 200) {
                setMessage({ text: response.data.message, severity: 'success' });
                setLog(response.data.transaction_log);
                setShowLogs(true);
            }
        } catch (error) {
            const errorDetail = error.response?.data?.detail?.error || "An unknown error occurred during Spark submit.";
            setMessage({ text: errorDetail, severity: 'error' });
        }
        setLoading(false);
    };

    const handleOpenLog = () => {
        setLogModalOpen(true);
    };

    const handleCloseLog = () => {
        setLogModalOpen(false);
    };

    return (
        <Container maxWidth="md">
            <Box sx={{ padding: 3, marginTop: 4, backgroundColor: '#f4f6f8', borderRadius: 2 }}>
                <Typography variant="h5" align="center" gutterBottom>
                    Bootstrap Hudi Table
                </Typography>
                <form onSubmit={handleSubmit}>
                    <Grid container spacing={2}>
                        <Grid item xs={12}>
                            <TextField
                                label="Data File Path"
                                name="data_file_path"
                                value={formData.data_file_path}
                                onChange={handleChange}
                                fullWidth
                                margin="normal"
                                required
                                size="small"
                            />
                        </Grid>
                        <Grid item xs={12}>
                            <TextField
                                label="Output Path"
                                name="output_path"
                                value={formData.output_path}
                                onChange={handleChange}
                                fullWidth
                                margin="normal"
                                required
                                size="small"
                            />
                        </Grid>
                        <Grid item xs={6}>
                            <TextField
                                label="Hudi Table Name"
                                name="hudi_table_name"
                                value={formData.hudi_table_name}
                                onChange={handleChange}
                                fullWidth
                                margin="normal"
                                required
                                size="small"
                            />
                        </Grid>
                        <Grid item xs={6}>
                            <Grid container spacing={1}>
                                <Grid item xs={8}>
                                    <TextField
                                        label="Enter Key Field"
                                        value={newKeyField}
                                        onChange={handleKeyFieldChange}
                                        fullWidth
                                        margin="normal"
                                        size="small"
                                        sx={{ maxWidth: '250px' }}
                                    />
                                </Grid>
                                <Grid item xs={4}>
                                    <Button
                                        onClick={handleAddKeyField}
                                        variant="contained"
                                        color="primary"
                                        disabled={!newKeyField}
                                        size="small"
                                        sx={{ mt: 2, height: '35px', minWidth: '100px' }}
                                    >
                                        Add Key Field
                                    </Button>
                                </Grid>
                            </Grid>
                        </Grid>
                        <Grid item xs={12}>
                            <Stack direction="row" spacing={1} sx={{ flexWrap: 'wrap', marginTop: 2 }}>
                                {formData.key_field.map((field, index) => (
                                    <Chip
                                        key={index}
                                        label={field}
                                        onDelete={() => handleDeleteKeyField(field)}
                                        color="primary"
                                        variant="outlined"
                                    />
                                ))}
                            </Stack>
                        </Grid>
                        <Grid item xs={6}>
                            <TextField
                                label="Precombine Field"
                                name="precombine_field"
                                value={formData.precombine_field}
                                onChange={handleChange}
                                fullWidth
                                margin="normal"
                                required
                                size="small"
                            />
                        </Grid>
                        <Grid item xs={6}>
                            <TextField
                                label="Partition Field"
                                name="partition_field"
                                value={formData.partition_field}
                                onChange={handleChange}
                                fullWidth
                                margin="normal"
                                required
                                size="small"
                            />
                        </Grid>
                        <Grid item xs={6}>
                            <FormControl fullWidth margin="normal" required>
                                <InputLabel>Hudi Table Type</InputLabel>
                                <Select
                                    name="hudi_table_type"
                                    value={formData.hudi_table_type}
                                    onChange={handleChange}
                                    size="small"
                                >
                                    <MenuItem value="COPY_ON_WRITE">COPY_ON_WRITE</MenuItem>
                                    <MenuItem value="MERGE_ON_READ">MERGE_ON_READ</MenuItem>
                                </Select>
                            </FormControl>
                        </Grid>
                        <Grid item xs={6}>
                            <FormControl fullWidth margin="normal" required>
                                <InputLabel>Write Operation</InputLabel>
                                <Select
                                    name="write_operation"
                                    value={formData.write_operation}
                                    onChange={handleChange}
                                    size="small"
                                >
                                    <MenuItem value="insert">Insert</MenuItem>
                                    <MenuItem value="upsert">Upsert</MenuItem>
                                </Select>
                            </FormControl>
                        </Grid>
                        <Grid item xs={6}>
                            <FormControl fullWidth margin="normal" required>
                                <InputLabel>Bootstrap Type</InputLabel>
                                <Select
                                    name="bootstrap_type"
                                    value={formData.bootstrap_type}
                                    onChange={handleChange}
                                    size="small"
                                >
                                    <MenuItem value="FULL_RECORD">FULL_RECORD</MenuItem>
                                    <MenuItem value="METADATA_ONLY">METADATA_ONLY</MenuItem>
                                </Select>
                            </FormControl>
                        </Grid>
                        <Grid item xs={6}>
                            <TextField
                                label="Partition Regex"
                                name="partition_regex"
                                value={formData.partition_regex}
                                onChange={handleChange}
                                fullWidth
                                margin="normal"
                                size="small"
                            />
                        </Grid>
                        <Grid item xs={12}>
                            <Box sx={{ textAlign: 'center', mt: 2 }}>
                                <Button
                                    variant="contained"
                                    color="primary"
                                    type="submit"
                                    disabled={loading}
                                    sx={{ width: '200px' }}
                                >
                                    {loading ? <CircularProgress size={24} /> : 'Bootstrap'}
                                </Button>
                            </Box>
                        </Grid>
                        {message.text && (
                            <Grid item xs={12}>
                                <Alert severity={message.severity} onClose={() => setMessage({ text: '', severity: '' })}>
                                    {message.text}
                                </Alert>
                            </Grid>
                        )}
                    </Grid>
                </form>
            </Box>
        </Container>
    );
}

export default App;
