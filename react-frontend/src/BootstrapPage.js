import React, { useState, useEffect } from 'react';import {    TextField,    Button,    Box,    Typography,    Alert,    CircularProgress,    Paper,    Grid,    MenuItem,    IconButton,} from '@mui/material';import { X } from '@phosphor-icons/react/dist/ssr/X';import { ArrowsClockwise } from '@phosphor-icons/react/dist/ssr/ArrowsClockwise';import axios from 'axios';import { useLocation } from 'react-router-dom';const defaultFormData = {    data_file_path: "",    hudi_table_name: "",    key_field: "",    precombine_field: "",    partition_field: "",    hudi_table_type: "COPY_ON_WRITE",    write_operation: "insert",    output_path: "",    spark_config: JSON.stringify({ 'spark.executor.memory': '2g' }, null, 2),    bootstrap_type: "FULL_RECORD",    partition_regex: ""};const BootstrapPage = ({ reset }) => {    const location = useLocation();    const [formData, setFormData] = useState(defaultFormData);    const [loading, setLoading] = useState(false);    const [checkLoading, setCheckLoading] = useState(false); // New state for check loading    const [message, setMessage] = useState({ text: '', severity: '' });    const [errorDetails, setErrorDetails] = useState(null);    const [submitted, setSubmitted] = useState(false);    const [errors, setErrors] = useState({});    const [isPartitioned, setIsPartitioned] = useState(null); // Track partitioning status    useEffect(() => {        if (location.state && location.state.formData) {            setFormData(location.state.formData);        } else {            setFormData(defaultFormData);        }    }, [location.state]);    useEffect(() => {        if (reset) {            setFormData(defaultFormData);            setIsPartitioned(null);        }    }, [reset]);    const handleChange = (e) => {        const { name, value, type, checked } = e.target;        setFormData((prev) => ({            ...prev,            [name]: type === 'checkbox' ? checked : value        }));        setErrors((prev) => ({ ...prev, [name]: null }));    };    const handleCheckPathOrTable = async () => {        const { data_file_path } = formData;	clearError();        setCheckLoading(true); // Start loading        try {            const response = await axios.post("http://127.0.0.1:8000/check_path_or_table/", { hudi_table_name: data_file_path });            const { isPartitioned, tableName, hdfsLocation } = response.data;            console.log(isPartitioned, tableName);            setIsPartitioned(isPartitioned);            if (tableName) {                setFormData((prev) => ({                    ...prev,		    data_file_path: hdfsLocation,                    hudi_table_name: tableName                }));            }        } catch (error) {            console.error("Error checking path or table:", error);            setMessage({ text: "Failed to check path/table.", severity: 'error' });        } finally {            setCheckLoading(false); // End loading        }    };    const handleSubmit = async (e) => {        e.preventDefault();	clearError();        setLoading(true);        setErrorDetails(null);        setSubmitted(true);        const newErrors = {};        const requiredFields = [            'data_file_path',            'output_path',            'hudi_table_name',            'key_field',            'precombine_field',        ];        requiredFields.forEach(field => {            if (!formData[field]) {                newErrors[field] = 'This field is required.';            }        });        // Partition field validation based on partitioning status        if (isPartitioned) {            if (!formData.partition_field) {                newErrors.partition_field = 'This field is required when data is partitioned.';            }        } else {            formData.partition_field = ''; // Clear if not partitioned            formData.partition_regex = ''; // Clear if not partitioned        }        setErrors(newErrors);        if (Object.keys(newErrors).length > 0) {            setLoading(false);            return;        }        try {            let sparkConfig = {};            try {                sparkConfig = formData.spark_config ? JSON.parse(formData.spark_config) : {};            } catch (jsonError) {                setErrorDetails({                    title: "JSON Parse Error",                    message: "Invalid Spark configuration JSON.",                    details: jsonError.message,                    timestamp: new Date().toISOString()                });                setLoading(false);                return;            }            const requestData = {                ...formData,                spark_config: sparkConfig            };            const response = await axios.post("http://127.0.0.1:8000/bootstrap_hudi/", requestData);            if (response.status === 200) {                setMessage({ text: response.data.message, severity: 'success' });            }        } catch (error) {            const errorMessage = error.response?.data?.message || "Failed to bootstrap the Hudi table.";            const errorDetails = error.response?.data || { message: error.message };            setMessage({ text: errorMessage, severity: 'error' });            setErrorDetails({                title: "Bootstrap Error",                message: errorMessage,                details: errorDetails,                timestamp: new Date().toISOString()            });        }        setLoading(false);    };    const clearError = () => {        setErrorDetails(null);        setMessage({ text: '', severity: '' });    };    return (        <Box sx={{             display: 'flex',             gap: 2,             padding: 2,            '& > *': {                minWidth: 0            }        }}>            <Box sx={{                 display: 'flex',                gap: 2,                flex: errorDetails ? 2 : 1,                mb: 2            }}>                <Box sx={{                     display: 'flex',                     flexDirection: 'column',                     gap: 2,                    flex: 1                }}>                    <Paper sx={{                         padding: 3,                        boxShadow: '0 0 20px rgba(0, 0, 0, 0.15)',                        display: 'flex',                        flexDirection: 'column'                    }}>                        <Typography variant="h6" gutterBottom sx={{ mb: 2 }}>Data Source & Destination</Typography>                        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>				<Grid container spacing={2}>				<Grid item xs={10.75}>                                <TextField                                    label="Data File Path or Hive Table"                                    name="data_file_path"                                    value={formData.data_file_path}                                    onChange={handleChange}                                    fullWidth                                    required                                    variant="outlined"                                    error={!!errors.data_file_path}                                    helperText={errors.data_file_path}                                    sx={{ marginBottom: 3 }}                                />				</Grid>				<Grid item xs={1.25}>                                <Button                                    variant="contained"                                    color="primary"                                    onClick={handleCheckPathOrTable}                                    sx={{ fontSize: '1rem', fontWeight: 500, padding: '8px 16px', marginTop: '1vh' }}                                    disabled={checkLoading} // Disable button while checking                                >                                    {checkLoading ? <CircularProgress size={24}  /> : <ArrowsClockwise size={24} weight="bold" />}                                </Button>				</Grid>				</Grid>                            </Box>                            <TextField                                label="Output Path"                                name="output_path"                                value={formData.output_path}                                onChange={handleChange}                                fullWidth                                required                                variant="outlined"                                error={!!errors.output_path}                                helperText={errors.output_path}                            />                        </Box>                    </Paper>                    <Paper sx={{                         padding: 3,                        boxShadow: '0 0 20px rgba(0, 0, 0, 0.15)',                        display: 'flex',                        flexDirection: 'column'                    }}>                        <Typography variant="h6" gutterBottom sx={{ mb: 2 }}>Table Configuration</Typography>                        <Box sx={{                             flex: 1,                             display: 'flex',                             flexDirection: 'column',                             gap: 3                        }}>                            <TextField                                label="Hudi Table Name"                                name="hudi_table_name"                                value={formData.hudi_table_name}                                onChange={handleChange}                                fullWidth                                required                                variant="outlined"                                error={!!errors.hudi_table_name}                                helperText={errors.hudi_table_name}                            />                            <TextField                                label="Operation Type"                                name="write_operation"                                value={formData.write_operation}                                onChange={handleChange}                                fullWidth                                select                                variant="outlined"                            >                                <MenuItem value="insert">Insert</MenuItem>                                <MenuItem value="upsert">Upsert</MenuItem>                            </TextField>                            <Grid container spacing={3}>                                <Grid item xs={6}>                                    <TextField                                        label="Table Type"                                        name="hudi_table_type"                                        value={formData.hudi_table_type}                                        onChange={handleChange}                                        fullWidth                                        select                                        variant="outlined"                                    >                                        <MenuItem value="COPY_ON_WRITE">COPY_ON_WRITE</MenuItem>                                        <MenuItem value="MERGE_ON_READ">MERGE_ON_READ</MenuItem>                                    </TextField>                                </Grid>                                <Grid item xs={6}>                                    <TextField                                        label="Bootstrap Type"                                        name="bootstrap_type"                                        value={formData.bootstrap_type}                                        onChange={handleChange}                                        fullWidth                                        select                                        variant="outlined"                                    >                                        <MenuItem value="FULL_RECORD">FULL_RECORD</MenuItem>                                        <MenuItem value="METADATA_ONLY">METADATA_ONLY</MenuItem>                                    </TextField>                                </Grid>                            </Grid>                            <TextField                                label="Partition Regex"                                name="partition_regex"                                value={formData.partition_regex}                                onChange={handleChange}                                fullWidth                                variant="outlined"                                disabled={!isPartitioned}                            />                        </Box>                    </Paper>                </Box>                <Paper sx={{                     padding: 3,                    boxShadow: '0 0 20px rgba(0, 0, 0, 0.15)',                    flex: 1,                    display: 'flex',                    flexDirection: 'column'                }}>                    <Typography variant="h6" gutterBottom sx={{ mb: 2 }}>Key Fields & Spark Configuration</Typography>                    <Box sx={{                         flex: 1,                        display: 'flex',                        flexDirection: 'column',                        gap: 3                    }}>                        <TextField                            label="Key Field"                            name="key_field"                            value={formData.key_field}                            onChange={handleChange}                            fullWidth                            required                            variant="outlined"                            error={!!errors.key_field}                            helperText={errors.key_field}                        />                        <TextField                            label="Precombine Field"                            name="precombine_field"                            value={formData.precombine_field}                            onChange={handleChange}                            fullWidth                            required                            variant="outlined"                            error={!!errors.precombine_field}                            helperText={errors.precombine_field}                        />                        <TextField                            label="Partition Field"                            name="partition_field"                            value={formData.partition_field}                            onChange={handleChange}                            fullWidth                            required={isPartitioned}                            variant="outlined"                            error={isPartitioned && !!errors.partition_field}                            helperText={isPartitioned && errors.partition_field}                            disabled={!isPartitioned}                        />                        <TextField                            label="Spark Config (JSON)"                            name="spark_config"                            value={formData.spark_config}                            onChange={handleChange}                            fullWidth                            multiline                            rows={4}                            variant="outlined"                        />                        <Box sx={{ textAlign: 'center', marginTop: 3 }}>                            <Button                                variant="contained"                                color="primary"                                type="submit"                                onClick={handleSubmit}                                disabled={loading}                                sx={{ fontSize: '1rem', fontWeight: 500, padding: '8px 24px' }}                            >                                {loading ? <CircularProgress size={24} /> : 'Submit'}                            </Button>                            {message.text && !errorDetails && (                                <Alert severity={message.severity} sx={{ marginTop: 3 }}>                                    {message.text}                                </Alert>                            )}                        </Box>                    </Box>                </Paper>            </Box>            {errorDetails && (                <Paper sx={{                     display: 'flex',                    flexDirection: 'column',                    boxShadow: '0 0 20px rgba(0, 0, 0, 0.15)',                    backgroundColor: '#FFF5F5',                    flex: 1,                    height: '640px',                }}>                    <Box sx={{                         p: 3,                         borderBottom: '1px solid rgba(0, 0, 0, 0.1)',                        display: 'flex',                        justifyContent: 'space-between',                        alignItems: 'center',                        backgroundColor: '#FFF5F5'                    }}>                        <Typography variant="h6" sx={{ color: '#E53E3E' }}>                            {errorDetails.title}                        </Typography>                        <IconButton onClick={clearError} size="small">                            <X size={20} />                        </IconButton>                    </Box>                    <Box sx={{                         p: 3,                        overflow: 'auto',                        flex: 1,                        display: 'flex',                        flexDirection: 'column',                        gap: 3,                        '&::-webkit-scrollbar': { width: '7px' },                        '&::-webkit-scrollbar-thumb': { backgroundColor: '#c0c0c0', borderRadius: '4px' },                        '&::-webkit-scrollbar-thumb:hover': { backgroundColor: '#f4f4f4' },                        '&::-webkit-scrollbar-track': { background: '#f1f1f1' },                    }}>                        <Alert severity="error" sx={{ flexShrink: 0 }}>                            {errorDetails.message}                        </Alert>                        <Box sx={{ flexShrink: 0 }}>                            <Typography variant="subtitle2" sx={{ mb: 1, color: '#666' }}>                                Error Details:                            </Typography>                            <Paper sx={{                                 p: 2,                                 backgroundColor: '#FFF',                            }}>                                <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>                                    {JSON.stringify(errorDetails.details, null, 2)}                                </pre>                            </Paper>                        </Box>                        <Typography variant="caption" sx={{ color: '#666', mt: 2 }}>                            Timestamp: {new Date(errorDetails.timestamp).toLocaleString()}                        </Typography>                    </Box>                </Paper>            )}        </Box>    );};export default BootstrapPage;