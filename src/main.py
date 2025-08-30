"""
Main ETL Orchestrator
Runs both GitHub and PyPI ETL processes in sequence
"""

import sys
import traceback
from datetime import datetime
import logging

# Import the ETL modules
try:
    import github_repo_etl
    import pypi_etl
    import load_public
except ImportError as e:
    print(f"Error importing ETL modules: {e}")
    print("Make sure github_repo_etl.py, pypi_etl.py, and load_public.py are in the same directory")
    sys.exit(1)

def setup_logging():
    """Configure logging for the main orchestrator"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('etl_orchestrator.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def run_github_etl(logger):
    """Run the GitHub repository ETL process"""
    logger.info("Starting GitHub ETL process...")
    try:
        github_repo_etl.main()
        logger.info("GitHub ETL completed successfully")
        return True
    except Exception as e:
        logger.error(f"GitHub ETL failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def run_pypi_etl(logger):
    """Run the PyPI download statistics ETL process"""
    logger.info("Starting PyPI ETL process...")
    try:
        pypi_etl.main()
        logger.info("PyPI ETL completed successfully")
        return True
    except Exception as e:
        logger.error(f"PyPI ETL failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def run_load_public(logger):
    """Run the metrics combination ETL process"""
    logger.info("Starting metrics combination ETL process...")
    try:
        load_public.main()
        logger.info("Metrics combination ETL completed successfully")
        return True
    except Exception as e:
        logger.error(f"Metrics combination ETL failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def main():
    """Main orchestrator function"""
    logger = setup_logging()
    
    start_time = datetime.now()
    logger.info("="*60)
    logger.info("Starting ETL Orchestrator")
    logger.info(f"Start time: {start_time}")
    logger.info("="*60)
    
    # Track results
    results = {
        'github_success': False,
        'pypi_success': False,
        'combine_success': False
    }
    
    # Run GitHub ETL
    logger.info("\n" + "="*40)
    logger.info("PHASE 1: GitHub Repository Metrics")
    logger.info("="*40)
    results['github_success'] = run_github_etl(logger)
    
    # Run PyPI ETL
    logger.info("\n" + "="*40)
    logger.info("PHASE 2: PyPI Download Statistics")
    logger.info("="*40)
    results['pypi_success'] = run_pypi_etl(logger)
    
    # Run Load Public
    logger.info("\n" + "="*40)
    logger.info("PHASE 3: Combining Metrics into Public Table")
    logger.info("="*40)
    results['combine_success'] = run_load_public(logger)
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("\n" + "="*60)
    logger.info("ETL ORCHESTRATOR SUMMARY")
    logger.info("="*60)
    logger.info(f"Start time: {start_time}")
    logger.info(f"End time: {end_time}")
    logger.info(f"Total duration: {duration}")
    logger.info("")
    logger.info("Process Results:")
    logger.info(f"  GitHub ETL: {'SUCCESS' if results['github_success'] else 'FAILED'}")
    logger.info(f"  PyPI ETL: {'SUCCESS' if results['pypi_success'] else 'FAILED'}")
    logger.info(f"  Load Public ETL: {'SUCCESS' if results['combine_success'] else 'FAILED'}")
    
    # Determine overall success
    overall_success = all(results.values())
    logger.info("")
    logger.info(f"Overall Status: {'SUCCESS' if overall_success else 'PARTIAL/FAILED'}")
    
    if not overall_success:
        logger.warning("Some ETL processes failed. Check the logs above for details.")
        failed_processes = [name.replace('_success', '').upper() 
                          for name, success in results.items() if not success]
        logger.warning(f"Failed processes: {', '.join(failed_processes)}")
    
    logger.info("="*60)
    
    # Exit with appropriate code
    sys.exit(0 if overall_success else 1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nETL process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error in main orchestrator: {e}")
        traceback.print_exc()
        sys.exit(1)