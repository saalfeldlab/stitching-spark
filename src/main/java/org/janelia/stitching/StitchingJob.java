package org.janelia.stitching;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;

import mpicbg.stitching.StitchingParameters;

/**
 * Represents input parameters and customizations for tweaking the stitching/fusing procedure.
 *
 * @author Igor Pisarev
 */

public class StitchingJob implements Serializable {

	public enum PipelineStep
	{
		Metadata, // mandatory step
		Blur,
		Shift,
		IntensityCorrection,
		Fusion,
		Export
	}

	private static final long serialVersionUID = 2619120742300093982L;

	private final EnumSet< PipelineStep > pipeline;
	private StitchingArguments args;
	private transient StitchingParameters params;
	private final String baseFolder;

	private String saveFolder;
	private String datasetName;

	private TileInfo[] tiles;
	private int dimensionality;


	public StitchingJob( final StitchingArguments args )
	{
		this.args = args;
		pipeline = createPipeline( args );

		final File inputFile = new File( args.inputFilePath() ).getAbsoluteFile();
		baseFolder = saveFolder = inputFile.getParent();
		datasetName = inputFile.getName();
		if ( datasetName.endsWith( ".json" ) )
			datasetName = datasetName.substring( 0, datasetName.lastIndexOf( ".json" ) );
	}

	protected StitchingJob()
	{
		pipeline = null;
		baseFolder = "";
	}

	private EnumSet< PipelineStep > createPipeline( final StitchingArguments args )
	{
		final List< PipelineStep > pipelineOnlyStepsList = new ArrayList<>();
		if ( args.onlyBlur() ) 					pipelineOnlyStepsList.add( PipelineStep.Blur );
		if ( args.onlyShift() ) 				pipelineOnlyStepsList.add( PipelineStep.Shift );
		if ( args.onlyIntensityCorrection() ) 	pipelineOnlyStepsList.add( PipelineStep.IntensityCorrection );
		if ( args.onlyFuse() ) 					pipelineOnlyStepsList.add( PipelineStep.Fusion );
		if ( args.onlyExport() ) 				pipelineOnlyStepsList.add( PipelineStep.Export );

		final List< PipelineStep > pipelineNoStepsList = new ArrayList<>();
		if ( args.noBlur() ) 					pipelineNoStepsList.add( PipelineStep.Blur );
		if ( args.noShift() ) 					pipelineNoStepsList.add( PipelineStep.Shift );
		if ( args.noIntensityCorrection() ) 	pipelineNoStepsList.add( PipelineStep.IntensityCorrection );
		if ( args.noFuse() ) 					pipelineNoStepsList.add( PipelineStep.Fusion );
		if ( args.noExport() ) 					pipelineNoStepsList.add( PipelineStep.Export );

		if ( !pipelineOnlyStepsList.isEmpty() && !pipelineNoStepsList.isEmpty() )
			throw new IllegalArgumentException( "Contradicting pipeline steps" );

		if ( !pipelineOnlyStepsList.isEmpty() )
			return EnumSet.of( PipelineStep.Metadata, pipelineOnlyStepsList.toArray( new PipelineStep[ 0 ] ) );
		else if ( !pipelineNoStepsList.isEmpty() )
			return EnumSet.complementOf( EnumSet.copyOf( pipelineNoStepsList ) );
		else
			return null;
	}

	public EnumSet< PipelineStep > getPipeline() { return pipeline; }

	public StitchingArguments getArgs() { return args; }

	public StitchingParameters getParams() { return params; }
	public void setParams( final StitchingParameters params ) { this.params = params; }

	public Map< Integer, TileInfo > getTilesMap()
	{
		final TreeMap< Integer, TileInfo > tilesMap = new TreeMap<>();
		for ( final TileInfo tile : tiles )
			tilesMap.put( tile.getIndex(), tile );
		return tilesMap;
	}

	public TileInfo[] getTiles() {
		return tiles;
	}

	public void setTiles( final TileInfo[] tiles ) throws Exception {
		this.tiles = tiles;
		checkTilesConfiguration();
	}

	public String getBaseFolder() { return baseFolder; }

	public String getSaveFolder() { return saveFolder; }
	public void setSaveFolder( final String saveFolder ) { this.saveFolder = saveFolder; }

	public String getDatasetName() { return datasetName; }

	public int getDimensionality() { return dimensionality; }

	public void validateTiles() throws IllegalArgumentException
	{
		if ( tiles.length < 2 )
			throw new IllegalArgumentException( "There must be at least 2 tiles in the dataset" );

		for ( int i = 0; i < tiles.length; i++ )
			if ( tiles[ i ].getPosition().length != tiles[ i ].getSize().length )
				throw new IllegalArgumentException( "Incorrect dimensionality" );

		for ( int i = 1; i < tiles.length; i++ )
			if ( tiles[ i ].numDimensions() != tiles[ i - 1 ].numDimensions() )
				throw new IllegalArgumentException( "Incorrect dimensionality" );

		// Everything is correct
		this.dimensionality = tiles[ 0 ].numDimensions();
		if ( params != null )
			params.dimensionality = dimensionality;
	}

	private void checkTilesConfiguration() throws Exception
	{
		boolean malformed = ( tiles == null );
		if ( !malformed )
			for ( final TileInfo tile : tiles )
				if ( tile == null )
					malformed = true;

		if ( malformed )
			throw new NullPointerException( "Malformed input" );

		for ( int i = 0; i < tiles.length; i++ ) {
			if ( tiles[ i ].getFilePath() == null || tiles[ i ].getPosition() == null )
				throw new NullPointerException( "Some of required parameters are missing (file or position)" );

			if ( tiles[ i ].getIndex() == null )
				tiles[ i ].setIndex( i );
		}
	}


	// TODO: pull request for making StitchingParameters serializable, then remove it
	private void writeObject( final ObjectOutputStream stream ) throws IOException {
		stream.defaultWriteObject();
		stream.write( new Gson().toJson( params ).getBytes() );
	}
	private void readObject( final ObjectInputStream stream ) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		params = new Gson().fromJson( IOUtils.toString(stream), StitchingParameters.class );
	}
}
